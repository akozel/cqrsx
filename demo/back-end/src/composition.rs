//! The tests of the box office assembled as `main.rs` assembles it, on the framework in memory: the settings of the
//! process and the values its modules read ([`process`]), its read side ([`read_side`]) and its tasks ([`tasks`]); what
//! the composition root and the process were tested for until the framework assembled them (backlog 05/12).
#[cfg(test)]
mod process;
#[cfg(test)]
mod read_side;
#[cfg(test)]
mod tasks;
