#!/usr/bin/env python3
"""The goal of this script is to compare and print the fields of a stream.

We used it to compare the fields mentioned in the QuickBooks documentation to the fields in the
tap's schema files.

How it works:

Prerequisites:

- Install this Chrome extension, [View Rendered Source extension][1]

  - We used this to download the rendered HTML version of the docs

    - The docs are a React app and using something like `curl` or `wget` gave us a file with a lot
      of JavaScript in it and none of the HTML tags we wanted

- `pip install beautifulsoup4`

Usage:
Get the HTML source:
- Open the [docs][2] in Chrome and use the extension to "View Rendered Source (Ctrl+Shift+U)"
- There should be 3 columns, but we only care about the middle "Rendered" one
- Click the "Copy" button in the top right corner of the "Rendered" column
- Paste this into a file
  - We used `vim` to write this file

Running the script:

- The command is `python compare_streams.py /path/to/json/schema/file /path/to/html/docs`

  - For example, we used `python compare_streams.py ../tap_quickbooks/schemas/vendors.json ../tap_quickbooks/schemas/vendors-html`
    from the `/spikes` directory
  - Note that `python` here is `python3`, using a virtualenv is highly recommended

- A file will be created in the directory you ran the command from named `missing-fields`
  - The output looks like:
    - a line with the path to the schema file you ran the command with
    - Maybe followed by tuples of this form:
      - `('field_name', 'inferred type from docs')`
      - For example, `('CustomerTypeRef', 'String')`

[1]: https://chrome.google.com/webstore/detail/view-rendered-source/ejgngohbdedoabanmclafpkoogegdpob?hl=en
[2]: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/account
"""
import re
import json
import sys

import bs4
from bs4 import BeautifulSoup

# Get file names to open
tap_file = sys.argv[1]
docs_schema = sys.argv[2]

# Soupify the HTML
with open(docs_schema, 'r') as infile:
    soup = BeautifulSoup(infile.read())

# Open the rendered html file and search for any tags with a `class` of `attribute-name`
# We noticed QuickBooks had non-unique field names, so we added the names to a set
matched_lines = set()
all_attr_name = soup.find_all(class_="attribute-name")
for element in all_attr_name:
    if isinstance(element, bs4.element.Tag):
        matched_lines.add(element.text)

# Load the JSON schema file and get the top level fields
with open(tap_file, 'r') as infile:
    tap_schema = json.load(infile)

tap_fields = set(tap_schema['properties'].keys())


# Do the set difference of the doc fields without the tap fields
missing_fields = matched_lines - tap_fields

# Try to infer the type of the field
# - Assumption: Each `attribute-name` tag shares a grandparent with an `attribute-type` tag
# - Assumption: There's one `attribute-type` tag per `attribute-name` tag
final_output = set()
for name in all_attr_name:
    if name.text in missing_fields:
        # We stringify the grandparent to pare down the HTML to the subtree that contains both
        # `attribute-name` and `attribute-type` elements. This way we can use `find_all()` again to
        # quickly search for `attribute-type`
        new_soup = BeautifulSoup(str(name.parent.parent))
        attr_type = new_soup.find_all(class_="attribute-type")
        # This `attr_type[0]` is where we assume `attribute-name` and `attribute-type` are
        # one-to-one
        final_output.add((name.text, attr_type[0].text))

# Save our findings to `missing-fields`
with open('missing-fields', 'a') as outfile:
    outfile.write(tap_file + '\n')
    for x in sorted(final_output, key=lambda v: v[0]):
        outfile.write(str(x) + '\n')
    outfile.write('\n')
