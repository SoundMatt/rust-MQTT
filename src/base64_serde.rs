// Copyright (c) 2026 Matt Jones. All rights reserved.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Base64 serde helpers for `Vec<u8>` fields (RELAY spec §15.1).

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<S>(bytes: &[u8], ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ser.serialize_str(&STANDARD.encode(bytes))
}

pub fn deserialize<'de, D>(de: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(de)?;
    STANDARD
        .decode(s.as_bytes())
        .map_err(serde::de::Error::custom)
}
