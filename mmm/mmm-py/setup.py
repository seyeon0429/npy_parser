import os
import sys
import sys

from setuptools import setup
from setuptools.command.sdist import sdist as SdistCommand
from setuptools_rust import RustExtension
from mmm import __version__

class CargoModifiedSdist(SdistCommand):
    """Modifies Cargo.toml to use an absolute rather than a relative path

    The current implementation of PEP 517 in pip always does builds in an
    isolated temporary directory. This causes problems with the build, because
    Cargo.toml necessarily refers to the current version of pyo3 by a relative
    path.

    Since these sdists are never meant to be used for anything other than
    tox / pip installs, at sdist build time, we will modify the Cargo.toml
    in the sdist archive to include an *absolute* path to pyo3.
    """

    def make_release_tree(self, base_dir, files):
        """Stages the files to be included in archives"""
        super().make_release_tree(base_dir, files)

        import toml

        # Cargo.toml is now staged and ready to be modified
        cargo_loc = os.path.join(base_dir, "Cargo.toml")
        assert os.path.exists(cargo_loc)

        with open(cargo_loc, "r") as f:
            cargo_toml = toml.load(f)

        rel_pyo3_path = cargo_toml["dependencies"]["pyo3"]["path"]
        base_path = os.path.dirname(__file__)
        abs_pyo3_path = os.path.abspath(os.path.join(base_path, rel_pyo3_path))

        cargo_toml["dependencies"]["pyo3"]["path"] = abs_pyo3_path

        with open(cargo_loc, "w") as f:
            toml.dump(cargo_toml, f)


def make_rust_extension(module_name, debug=True):
    return RustExtension(module_name, "Cargo.toml", debug=debug)


debug = False
if sys.argv[1] == "debug":
    debug = True
    sys.argv[1] = "install"
elif sys.argv[1] == "release":
    sys.argv[1] = "install"

install_requires = []

setup(
    name="mmm",
    version=__version__,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Rust",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
    ],
    packages=["mmm", "mmm.nasdaq","mmm.nyse"],
    rust_extensions=[
        make_rust_extension("mmm.nasdaq_py", debug=debug),
        make_rust_extension("mmm.nyse_py", debug=debug),
    ],
    install_requires=install_requires,
    include_package_data=True,
    zip_safe=False,
    cmdclass={"sdist": CargoModifiedSdist},
)
