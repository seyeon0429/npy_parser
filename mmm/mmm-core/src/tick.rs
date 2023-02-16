use std::ops::{AddAssign, Rem, SubAssign};

pub enum Direction {
    Up,
    Down,
    None,
}

pub trait PriceTick: Default + Rem<Output = Self> + Eq + AddAssign + SubAssign + Clone {
    #[must_use]
    fn tickify(&self, direction: Direction) -> Self;
    fn valid_price(&self) -> bool {
        (self.clone() % self.tickify(Direction::None)) == Self::default()
    }
    #[must_use]
    fn ceiling_price(mut self) -> Self {
        self += self.tickify(Direction::Up);
        self
    }
    #[must_use]
    fn floor_price(mut self) -> Self {
        self -= self.tickify(Direction::Down);
        self
    }
}
