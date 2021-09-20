// Copyright (c) 2017-2019, Substratum LLC (https://substratum.net) and/or its affiliates. All rights reserved.

use clap::ArgEnum;

#[derive(ArgEnum, Debug, PartialEq, Clone, Copy)]
    pub enum CrashPoint {
        Message,
        Panic,
        Error,
        None,
    }

const NONE: usize = 0;
const PANIC: usize = 1;
const ERROR: usize = 2;
const MESSAGE: usize = 3;

impl From<usize> for CrashPoint {
    fn from(number: usize) -> Self {
        match number {
            PANIC => CrashPoint::Panic,
            ERROR => CrashPoint::Error,
            MESSAGE => CrashPoint::Message,
            _ => CrashPoint::None,
        }
    }
}

impl From<CrashPoint> for usize {
    fn from(crash_point: CrashPoint) -> Self {
        match crash_point {
            CrashPoint::Message => MESSAGE,
            CrashPoint::Panic => PANIC,
            CrashPoint::Error => ERROR,
            CrashPoint::None => NONE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_usize_to_crash_point() {
        assert_eq!(CrashPoint::from(NONE), CrashPoint::None);
        assert_eq!(CrashPoint::from(PANIC), CrashPoint::Panic);
        assert_eq!(CrashPoint::from(ERROR), CrashPoint::Error);
        assert_eq!(CrashPoint::from(MESSAGE), CrashPoint::Message);
    }

    #[test]
    fn from_crash_point_to_usize() {
        let none = usize::from(CrashPoint::None);
        let panic = usize::from(CrashPoint::Panic);
        let error = usize::from(CrashPoint::Error);
        let message = usize::from(CrashPoint::Message);

        assert_eq!(none, NONE);
        assert_eq!(panic, PANIC);
        assert_eq!(error, ERROR);
        assert_eq!(message, MESSAGE);
    }
}
