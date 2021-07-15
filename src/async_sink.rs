// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::Result;
use async_channel::Receiver;
use std::collections::VecDeque;
use std::error;
use std::fmt;
use std::result;

/// Adding support for sinks that handle events asynchronously with a
/// given `capacity` of events handles concurrently
#[derive(Debug)]
pub struct AsyncSink<T> {
    queue: VecDeque<Receiver<Result<T>>>,
    capacity: usize,
}

#[derive(Debug, PartialEq)]
pub enum SinkEnqueueError {
    AtCapacity,
}
impl error::Error for SinkEnqueueError {}

impl fmt::Display for SinkEnqueueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Error result of the attempt to dequeue a slot for a new event ot handle
#[derive(Debug, PartialEq)]
pub enum SinkDequeueError {
    /// the queue is empty
    Empty,
    /// the dequeued slot is filled and the result is not yet there
    NotReady,
}
impl error::Error for SinkDequeueError {}

impl fmt::Display for SinkDequeueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// A queue of async tasks defined by an receiver that returns once the task
/// completes.
impl<T> AsyncSink<T> {
    /// constructor
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            capacity,
        }
    }
    /// enqueue a receiver for a result of an async event handling
    pub fn enqueue(&mut self, value: Receiver<Result<T>>) -> result::Result<(), SinkEnqueueError> {
        if self.queue.len() >= self.capacity {
            Err(SinkEnqueueError::AtCapacity)
        } else {
            self.queue.push_back(value);
            Ok(())
        }
    }
    /// attempt to dequeue the result of an async event handling
    pub fn dequeue(&mut self) -> result::Result<Result<T>, SinkDequeueError> {
        match self.queue.pop_front() {
            None => Err(SinkDequeueError::Empty),
            Some(rx) => match rx.try_recv() {
                Err(_) => {
                    self.queue.push_front(rx);
                    Err(SinkDequeueError::NotReady)
                }
                Ok(result) => Ok(result),
            },
        }
    }

    /// returns true if the `AsyncSink` has capacity for at least one addition event handling job
    pub fn has_capacity(&self) -> bool {
        self.queue.len() < self.capacity
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use async_channel::bounded;

    #[test]
    fn dequeue_empty() {
        let mut q: AsyncSink<u8> = AsyncSink::new(2);
        assert_eq!(q.dequeue(), Err(SinkDequeueError::Empty));
    }

    #[async_std::test]
    async fn full_cycle() {
        let mut q: AsyncSink<u8> = AsyncSink::new(2);
        let (tx1, rx) = bounded(1);
        assert!(q.enqueue(rx).is_ok());
        let (tx2, rx) = bounded(1);
        assert!(q.enqueue(rx).is_ok());
        let (_tx3, rx) = bounded(1);
        assert_eq!(q.enqueue(rx).err(), Some(SinkEnqueueError::AtCapacity));
        assert_eq!(q.dequeue(), Err(SinkDequeueError::NotReady));
        assert!(tx1.send(Ok(1)).await.is_ok());
        assert_eq!(q.dequeue(), Ok(Ok(1)));
        let (tx4, rx) = bounded(1);
        assert!(q.enqueue(rx).is_ok());
        let (_tx5, rx) = bounded(1);
        assert_eq!(q.enqueue(rx).err(), Some(SinkEnqueueError::AtCapacity));
        assert_eq!(q.dequeue(), Err(SinkDequeueError::NotReady));
        assert!(tx2.send(Ok(2)).await.is_ok());
        assert_eq!(q.dequeue(), Ok(Ok(2)));
        assert!(tx4.send(Ok(4)).await.is_ok());
        assert_eq!(q.dequeue(), Ok(Ok(4)));
        assert_eq!(q.dequeue(), Err(SinkDequeueError::Empty));
    }
}
