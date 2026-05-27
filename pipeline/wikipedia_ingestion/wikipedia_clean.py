import re

MIN_WORD_COUNT  = 300
MIN_ALPHA_RATIO = 0.70

NON_CONTENT_SECTION_HEADERS = re.compile(
    r"^==+\s*("
    r"See also"
    r"|References"
    r"|Further reading"
    r"|External links"
    r"|Notes"
    r"|Bibliography"
    r"|Footnotes"
    r"|Citations"
    r"|Sources"
    r"|Works cited"
    r"|Related pages"
    r"|Read more"
    r"|Navigation menu"
    r"|Contents"
    r"|Appendix"
    r"|Appendices"
    r"|Index"
    r")\s*==+\s*$",
    re.IGNORECASE | re.MULTILINE,
)

NAV_LINE = re.compile(
    r"^("
    r"Categories\s*:"
    r"|Retrieved from"
    r"|This page was last"
    r"|Wikipedia®"
    r"|Text is available"
    r"|Privacy policy"
    r"|About Wikipedia"
    r"|Disclaimers"
    r"|Contact Wikipedia"
    r"|Mobile view"
    r"|Developers"
    r"|Cookie statement"
    r"|\{\{.*\}\}"
    r"|\[\[.*\]\]"
    r")",
    re.IGNORECASE,
)

BARE_LIST_LINE = re.compile(r"^\*\s*\[\[.+?\]\]\s*$")
SECTION_HEADER = re.compile(r"^==+\s*.+?\s*==+\s*$")

PLAIN_FOOTER = re.compile(
    r"\n\n("
    r"See also"
    r"|References"
    r"|Further reading"
    r"|External links"
    r"|Notes"
    r"|Bibliography"
    r"|Footnotes"
    r"|Citations"
    r"|Sources"
    r"|Works cited"
    r"|Related pages"
    r")\n.*",
    re.IGNORECASE | re.DOTALL,
)


def strip_non_content_sections(text: str) -> str:
    match = NON_CONTENT_SECTION_HEADERS.search(text)
    if match:
        text = text[:match.start()]

    text = PLAIN_FOOTER.sub("", text)
    text = re.sub(r"\n+[a-z]{2,3}:[^\n]+$", "", text, flags=re.MULTILINE)

    lines = text.rstrip().split("\n")
    while lines:
        last = lines[-1].strip()
        if last and re.match(r"^[A-Za-z][a-zA-Z\s]{0,49}$", last) \
                and "." not in last and len(last) < 50:
            lines.pop()
        else:
            break

    return "\n".join(lines).strip()


def clean_article(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""

    text = strip_non_content_sections(text)
    text = re.sub(r"[\u00a0\u200b\u200c\u200d\u2028\u2029\ufeff]", " ", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines         = text.split("\n")
    cleaned_lines = []

    for line in lines:
        stripped = line.strip()

        if not stripped:
            cleaned_lines.append("")
            continue

        if NAV_LINE.match(stripped):
            continue

        if BARE_LIST_LINE.match(stripped):
            continue

        if SECTION_HEADER.match(stripped):
            header_text = re.sub(r"^=+\s*", "", stripped)
            header_text = re.sub(r"\s*=+$", "", header_text).strip()
            if header_text:
                cleaned_lines.append(f"\n{header_text}")
            continue

        stripped = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]+)\]\]", r"\1", stripped)
        stripped = re.sub(r"\[\d+\]", "", stripped)
        stripped = re.sub(r"\[citation needed\]", "", stripped, flags=re.IGNORECASE)
        stripped = re.sub(r"\[note \d+\]", "", stripped, flags=re.IGNORECASE)
        stripped = re.sub(r"\{\{[^}]*\}\}", "", stripped)
        stripped = re.sub(r"[ \t]+", " ", stripped).strip()

        if stripped:
            cleaned_lines.append(stripped)

    collapsed  = []
    prev_blank = False
    for line in cleaned_lines:
        is_blank = line.strip() == ""
        if is_blank:
            if not prev_blank:
                collapsed.append("")
            prev_blank = True
        else:
            collapsed.append(line)
            prev_blank = False

    text        = "\n".join(collapsed).strip()
    final_lines = []

    for line in text.split("\n"):
        s = line.strip()
        if not s:
            final_lines.append("")
            continue
        alpha_count = sum(c.isalpha() for c in s)
        if len(s.replace(" ", "")) == 0 or alpha_count / max(len(s.replace(" ", "")), 1) >= 0.40:
            final_lines.append(line)

    return "\n".join(final_lines).strip()


def is_quality_article(text: str) -> bool:
    if not text:
        return False

    if re.search(r"\bmay refer to\b|\bdisambiguation\b", text[:300], re.IGNORECASE):
        return False

    if re.match(r"^#redirect", text, re.IGNORECASE):
        return False

    if len(text.split()) < MIN_WORD_COUNT:
        return False

    alpha = sum(c.isalpha() for c in text)
    if len(text) == 0 or alpha / len(text) < MIN_ALPHA_RATIO:
        return False

    return True
