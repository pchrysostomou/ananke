use crate::Rng;

/// OS entropy via `getrandom`. Stateless: every call is a fresh draw from the kernel.
#[derive(Clone, Copy, Debug, Default)]
pub struct RealRng;

impl Rng for RealRng {
    fn fill_bytes(&self, dest: &mut [u8]) {
        getrandom::fill(dest).expect("OS entropy source unavailable");
    }
}
