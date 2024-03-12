use chrono::NaiveDateTime;

// hacky workaround to estimate current state
#[derive(Default, Clone, Copy)]
pub struct ChainState {
    start: NaiveDateTime,
    block_number: u64,
}

impl ChainState {
    pub fn new(start: NaiveDateTime, block_number: u64) -> Self {
        Self { start, block_number }
    }
    pub async fn current_block(&self) -> u64 {
        let now = chrono::Local::now().naive_utc();
        let diff = now.signed_duration_since(self.start);
        let blocks_passed = (diff.num_seconds() / 12) as u64;
        self.block_number + blocks_passed
    }
}
