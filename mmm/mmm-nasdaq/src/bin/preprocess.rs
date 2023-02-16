use std::path::PathBuf;

use mmm_nasdaq::{
    book::{NasdaqOrderBook,Message},
    data::{load, process_file,  NUM_FIELDS},
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use structopt::StructOpt;

#[derive(StructOpt)]
enum Opt {
    Prep {
        #[structopt(name = "FILE", parse(from_os_str))]
        files: Vec<PathBuf>,
        #[structopt(short, long)]
        out_dir: PathBuf,
        #[structopt(long)]
        no_cache: bool,
    },
    Recon {
        #[structopt(name = "FILE", parse(from_os_str))]
        files: Vec<PathBuf>,
        #[structopt(long)]
        without_validation: bool,
    },
}

pub fn reconstruct(path: PathBuf, validation: bool) {
    let msgs = load(&path, NUM_FIELDS).into_iter().map(|bytes| Message::from(&*bytes));
    let mut engine = NasdaqOrderBook::new(validation);

    for msg in msgs {
        if engine.handle(&msg).is_err() {
            return;
        };
    }
}

fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::from_args();
    match opt {
        Opt::Prep {
            files,
            out_dir,
            no_cache,
        } => {
            if no_cache {
                let _ = std::fs::remove_dir_all(&out_dir);
            }
            let _ = std::fs::create_dir_all(&out_dir);

            files
                .into_par_iter()
                .map(|path| process_file(path, out_dir.clone(), false))
                .collect::<Vec<_>>();
        }
        Opt::Recon {
            files,
            without_validation,
        } => {
            files
                .into_par_iter()
                .map(|file| reconstruct(file, !without_validation))
                .collect::<Vec<_>>();
        }
    }
    Ok(())
}
