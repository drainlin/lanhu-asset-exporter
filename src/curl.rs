use anyhow::{Context, Result, bail};
use reqwest::header::{COOKIE, HeaderMap, HeaderName, HeaderValue};
use url::Url;

pub struct CurlRequest {
    pub list_url: Url,
    pub headers: HeaderMap,
}

/// Parses curl arguments without ever invoking a shell or executing the input.
pub fn parse_list_curl(input: &str) -> Result<CurlRequest> {
    let tokens = shell_words::split(input).context("curl input has unmatched quotes")?;
    if tokens.first().is_none_or(|token| token != "curl") {
        bail!("paste a curl command beginning with `curl`");
    }

    let mut url = None;
    let mut headers = HeaderMap::new();
    let mut index = 1;
    while index < tokens.len() {
        let token = &tokens[index];
        let value = |index: &mut usize| -> Result<&str> {
            *index += 1;
            tokens
                .get(*index)
                .map(String::as_str)
                .context("curl flag is missing a value")
        };
        match token.as_str() {
            "--url" => url = Some(value(&mut index)?.to_owned()),
            "-H" | "--header" => add_header(&mut headers, value(&mut index)?)?,
            "-b" | "--cookie" => {
                let cookie = HeaderValue::from_str(value(&mut index)?)?;
                headers.insert(COOKIE, cookie);
            }
            _ if token.starts_with("--url=") => url = Some(token[6..].to_owned()),
            _ if token.starts_with("--cookie=") => {
                let cookie = HeaderValue::from_str(&token[9..])?;
                headers.insert(COOKIE, cookie);
            }
            _ if token.starts_with("http://") || token.starts_with("https://") => {
                url = Some(token.to_owned());
            }
            _ => {}
        }
        index += 1;
    }

    let list_url = Url::parse(&url.context("curl command has no URL")?)?;
    if !is_lanhu_host(&list_url) || list_url.path() != "/api/project/images" {
        bail!("the URL must be a lanhuapp.com /api/project/images request");
    }
    Ok(CurlRequest { list_url, headers })
}

#[cfg(test)]
mod tests {
    use super::parse_list_curl;

    #[test]
    fn parses_url_headers_and_cookie_without_executing_it() {
        let request = parse_list_curl("curl --url 'https://lanhuapp.com/api/project/images?project_id=p&team_id=t' -H 'accept: application/json' -b 'session=redacted'").unwrap();
        assert_eq!(request.list_url.path(), "/api/project/images");
        assert_eq!(request.headers.get("accept").unwrap(), "application/json");
        assert_eq!(request.headers.get("cookie").unwrap(), "session=redacted");
    }

    #[test]
    fn rejects_non_lanhu_targets() {
        assert!(parse_list_curl("curl --url https://example.com/api/project/images").is_err());
    }
}

fn add_header(headers: &mut HeaderMap, raw: &str) -> Result<()> {
    let (name, value) = raw
        .split_once(':')
        .context("header must use `Name: value`")?;
    let name = HeaderName::from_bytes(name.trim().as_bytes())?;
    if matches!(name.as_str(), "host" | "content-length") {
        return Ok(());
    }
    headers.append(name, HeaderValue::from_str(value.trim())?);
    Ok(())
}

pub fn is_lanhu_host(url: &Url) -> bool {
    matches!(url.host_str(), Some("lanhuapp.com"))
        || url
            .host_str()
            .is_some_and(|host| host.ends_with(".lanhuapp.com"))
}
