use telegram_markdown_v2::{UnsupportedTagsStrategy, convert_with_strategy};

pub(super) fn markdown_to_telegram_markdown_v2(
    markdown: &str,
) -> telegram_markdown_v2::Result<String> {
    let mut chunks = Vec::new();
    let mut prose = Vec::new();
    let mut list = Vec::new();

    for line in markdown.lines() {
        if parse_list_line(line).is_some() {
            flush_prose(&mut prose, &mut chunks)?;
            list.push(line);
        } else {
            flush_list(&mut list, &mut chunks)?;
            prose.push(line);
        }
    }

    flush_prose(&mut prose, &mut chunks)?;
    flush_list(&mut list, &mut chunks)?;

    if chunks.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!("{}\n", chunks.join("\n\n")))
    }
}

fn flush_prose(
    prose: &mut Vec<&str>,
    chunks: &mut Vec<String>,
) -> telegram_markdown_v2::Result<()> {
    if prose.is_empty() {
        return Ok(());
    }

    let markdown = prose.join("\n");
    let rendered = convert_fragment(&markdown)?;
    if !rendered.is_empty() {
        chunks.push(rendered);
    }
    prose.clear();

    Ok(())
}

fn flush_list(list: &mut Vec<&str>, chunks: &mut Vec<String>) -> telegram_markdown_v2::Result<()> {
    if list.is_empty() {
        return Ok(());
    }

    let mut lines = Vec::new();
    for line in list.iter() {
        if let Some(item) = parse_list_line(line) {
            lines.push(render_list_item(item)?);
        }
    }

    if !lines.is_empty() {
        chunks.push(lines.join("\n"));
    }
    list.clear();

    Ok(())
}

fn render_list_item(item: ListLine<'_>) -> telegram_markdown_v2::Result<String> {
    let content = convert_fragment(item.content.trim())?;
    let indent = "    ".repeat(item.level);
    let marker = match item.marker {
        ListMarker::Unordered => "•   ".to_string(),
        ListMarker::Ordered(number) => format!("{number}\\.  "),
    };

    Ok(format!("{indent}{marker}{content}"))
}

fn convert_fragment(markdown: &str) -> telegram_markdown_v2::Result<String> {
    Ok(
        convert_with_strategy(markdown, UnsupportedTagsStrategy::Escape)?
            .trim_end()
            .to_string(),
    )
}

fn parse_list_line(line: &str) -> Option<ListLine<'_>> {
    let indent = line
        .chars()
        .take_while(|character| *character == ' ')
        .count();
    let level = indent / 2;
    let trimmed = line.trim_start();

    if let Some(content) = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
    {
        return Some(ListLine {
            level,
            marker: ListMarker::Unordered,
            content,
        });
    }

    let (number, content) = parse_ordered_marker(trimmed)?;
    Some(ListLine {
        level,
        marker: ListMarker::Ordered(number),
        content,
    })
}

fn parse_ordered_marker(line: &str) -> Option<(usize, &str)> {
    let marker_end = line.find(". ")?;
    if marker_end == 0 {
        return None;
    }

    let number = line[..marker_end].parse().ok()?;
    Some((number, &line[marker_end + 2..]))
}

struct ListLine<'a> {
    level: usize,
    marker: ListMarker,
    content: &'a str,
}

enum ListMarker {
    Unordered,
    Ordered(usize),
}

#[cfg(test)]
mod tests {
    use super::markdown_to_telegram_markdown_v2;

    #[test]
    fn converts_common_markdown_to_telegram_markdown_v2() {
        let output = markdown_to_telegram_markdown_v2("**bold** and *italic* with `code`").unwrap();

        assert_eq!(output, "*bold* and _italic_ with `code`\n");
    }

    #[test]
    fn escapes_plain_text_reserved_symbols() {
        let output = markdown_to_telegram_markdown_v2("Use <tag> & safe!").unwrap();

        assert_eq!(output, "Use <tag\\> & safe\\!\n");
    }

    #[test]
    fn converts_links() {
        let output =
            markdown_to_telegram_markdown_v2("[docs](https://example.com?a=1&b=x)").unwrap();

        assert_eq!(output, "[docs](https://example.com?a=1&b=x)\n");
    }

    #[test]
    fn converts_nested_markup_inside_links() {
        let output = markdown_to_telegram_markdown_v2("[**docs**](https://example.com)").unwrap();

        assert_eq!(output, "[*docs*](https://example.com)\n");
    }

    #[test]
    fn keeps_empty_links_as_plain_text() {
        let output = markdown_to_telegram_markdown_v2("[docs]()").unwrap();

        assert_eq!(output, "docs\n");
    }

    #[test]
    fn converts_fenced_code_blocks() {
        let output =
            markdown_to_telegram_markdown_v2("```rust\nfn main() { println!(\"<ok>\"); }\n```")
                .unwrap();

        assert_eq!(output, "```\nfn main() { println!(\"<ok>\"); }\n```\n");
    }

    #[test]
    fn converts_lists() {
        let output =
            markdown_to_telegram_markdown_v2("- one\n- two\n\n1. first\n2. second").unwrap();

        assert_eq!(output, "•   one\n•   two\n\n1\\.  first\n2\\.  second\n");
    }

    #[test]
    fn converts_nested_unordered_lists() {
        let output =
            markdown_to_telegram_markdown_v2("- parent\n  - child\n  - child two\n- sibling")
                .unwrap();

        assert_eq!(
            output,
            "•   parent\n    •   child\n    •   child two\n•   sibling\n"
        );
    }

    #[test]
    fn converts_nested_ordered_lists() {
        let output =
            markdown_to_telegram_markdown_v2("1. parent\n   1. child\n   2. child two\n2. sibling")
                .unwrap();

        assert_eq!(
            output,
            "1\\.  parent\n    1\\.  child\n    2\\.  child two\n2\\.  sibling\n"
        );
    }

    #[test]
    fn escapes_unsupported_constructs() {
        let output = markdown_to_telegram_markdown_v2("> quoted").unwrap();

        assert_eq!(output, "\\> quoted\n");
    }
}
