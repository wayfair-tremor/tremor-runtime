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
/// Using `GOOGLE_APPLICATION_CREDENTIALS="<path to service token json file>"` this function
/// will authenticate against the google cloud platform using the authentication flow defined
/// in the file. The provided PEM file should be a current non-revoked and complete copy of the
/// required certificate chain for the Google Cloud Platform.
use gouth::Token;
use reqwest::header::HeaderMap;
use reqwest::Client;
use halfbrown::HashMap;

pub(crate) async fn authenticate_bearer() -> Result<String> {
    Ok(Token::new()?.header_value()?.to_string())
}

pub(crate) async fn json_api_client(extra_headers: &HeaderMap) -> Result<Client> {
  let bearer = authenticate_bearer().await?;
  let mut headers = reqwest::header::HeaderMap::new();
  headers.insert(
      "authorization",
      reqwest::header::HeaderValue::from_maybe_shared(bearer)?,
  );
  headers.insert(
      "content-type",
      reqwest::header::HeaderValue::from_static("application/json"),
  );
  for header in extra_headers {
      headers.append(header.0, header.1.clone());
  }  
  Ok(reqwest::Client::builder()
        .default_headers(headers)
        .build()?)
}

pub(crate) async fn merge_gcp_headers(extra_headers: &HashMap<String,String>) -> Result<HashMap<String, String>> {
  let bearer = authenticate_bearer().await?;
  let mut headers: HashMap<String,String> = halfbrown::HashMap::new();
  headers.insert(
      "authorization".to_string(),
      bearer,
  );
  headers.insert(
      "content-type".to_string(),
      "application/json".to_string(),
  );
  for header in extra_headers { // should be a small list so clone em all, I say.
      headers.insert(header.0.clone().to_string(), header.1.clone().to_string());
  }
  Ok(headers)
}