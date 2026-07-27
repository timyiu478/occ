pub mod parallel;
pub mod serial;

use crate::error::OccError;
use crate::transaction::Transaction;

pub trait OccEngine<'a, K, V>
where
    K: 'a,
    V: 'a,
{
    fn begin(&'a self) -> Transaction<'a, K, V>;
    fn commit(&self, tx: &mut Transaction<'a, K, V>) -> Result<(), OccError>;

    fn transaction<F, R>(&'a self, f: F) -> Result<R, OccError>
    where
        F: FnOnce(&mut Transaction<'a, K, V>) -> Result<R, OccError>,
    {
        let mut tx = self.begin();
        let result = f(&mut tx)?;
        self.commit(&mut tx)?;
        Ok(result)
    }
}
