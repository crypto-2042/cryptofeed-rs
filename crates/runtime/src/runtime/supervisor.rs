pub struct Backoff {
    current: u64,
    max: u64,
}

impl Backoff {
    pub fn new(initial: u64, max: u64) -> Self {
        Self { current: initial, max }
    }

    pub fn next_delay_secs(&mut self) -> u64 {
        let value = self.current;
        self.current = (self.current * 2).min(self.max);
        value
    }
}

#[cfg(test)]
mod tests {
    use super::Backoff;

    #[test]
    fn backoff_doubles_until_cap() {
        let mut backoff = Backoff::new(1, 8);
        assert_eq!(backoff.next_delay_secs(), 1);
        assert_eq!(backoff.next_delay_secs(), 2);
        assert_eq!(backoff.next_delay_secs(), 4);
        assert_eq!(backoff.next_delay_secs(), 8);
        assert_eq!(backoff.next_delay_secs(), 8);
    }
}
