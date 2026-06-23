Based on the pull request in `singer-io/tap-amazon-ads`, I can see this is a comprehensive change that adds access control checking for streams during discovery. Here's a detailed prompt you can use with an AI agent to apply similar changes to other Singer tap connectors.

> **Recommended application order:** Apply changes one section at a time — (1) stream base class, (2) `discover.py`, (3) `__init__.py`, (4) tests. Complete and verify each section before proceeding to the next to avoid losing track of earlier instructions.

---

## AI Agent Prompt: Implement Unauthorized Stream Exclusion During Discovery

### Objective
Modify a Singer tap connector to gracefully handle HTTP 403 (Forbidden) errors during discovery by excluding unauthorized streams from the catalog instead of failing entirely. This pattern allows partial catalog discovery when credentials lack access to certain resources.

### Background
In Singer taps that connect to APIs with granular permissions, credentials may have access to only a subset of available streams. The default behavior causes discovery to fail entirely when encountering a 403 error. This enhancement excludes inaccessible streams from the catalog and continues with authorized streams.

### Reference Implementation
See PR #12 in `singer-io/tap-amazon-ads`: https://github.com/singer-io/tap-amazon-ads/pull/12

### Changes Required

#### 1. **Update Discovery Flow** (`tap_<name>/discover.py`)

**Add access check infrastructure:**
```python
def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.
    Note: check_access() always returns True for child streams, so this loop
    effectively identifies only inaccessible parent streams by design.
    Child stream removal is handled separately by _prune_inaccessible_children().
    Raises <Provider>ForbiddenError if no parent streams are accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_obj in STREAMS.items()
        if stream_name in schemas
        and not stream_obj(client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise <Provider>ForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )
    elif inaccessible_streams:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(inaccessible_streams),
        )


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    for name, stream_cls in list(STREAMS.items()):
        if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name, stream_cls.parent,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)
```

**Modify the discover() function:**
- Change signature from `def discover()` to `def discover(client)`
- Call `_apply_access_checks(client, schemas, field_metadata)` after `get_schemas()` but before building the catalog
- Example:
```python
def discover(client) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    Access to each stream is verified using the provided client and streams
    the credentials cannot read are excluded from the returned catalog.
    """
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])
    # ... rest of catalog building logic
```

#### 2. **Update Main Entry Point** (`tap_<name>/__init__.py`)

**Modify do_discover() to accept client:**
```python
def do_discover(client):
    """
    Discover and emit the catalog to stdout
    """
    LOGGER.info("Starting discover")
    catalog = discover(client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")
```

**Update main() to pass client to do_discover:**
```python
def main():
    parsed_args = parse_args(REQUIRED_CONFIG_KEYS)

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(client)  # Changed from do_discover()
        elif parsed_args.catalog:
            # ... sync logic
```

#### 3. **Add check_access() Method to Base Stream** (`tap_<name>/streams/abstracts.py`)

Add the following method to your `BaseStream` class:

```python
def check_access(self) -> bool:
    """
    Verify that the API credentials have read access to this stream.
    Returns True if accessible, False if a 403 Forbidden error is raised.
    Child streams always return True (access is governed by the parent check).
    This means children are never flagged by the access-check loop in _apply_access_checks();
    their removal from the catalog is handled separately by _prune_inaccessible_children().
    """
    if self.parent:
        return True

    url = self.get_url_endpoint()
    self.update_params()

    if self.http_method == "POST":
        self.update_data_payload()
        body = json.dumps(self.data_payload)
    else:
        body = None

    try:
        self.client.make_request(
            self.http_method,
            url,
            self.params,
            self.headers,
            body=body,
        )
        return True
    except <Provider>ForbiddenError as exc:
        LOGGER.warning(
            "Permission Error: Stream '%s' - %s",
            self.__class__.__name__,
            exc,
        )
        return False
```

**Update BaseStream.__init__() to handle None catalog/client:**
```python
def __init__(self, client=None, catalog=None) -> None:
    self.client = client
    self.catalog = catalog
    self.schema = catalog.schema.to_dict() if catalog else {}
    self.metadata = metadata.to_map(catalog.metadata) if catalog else {}
    self.child_to_sync = []
    self.params = {}
    self.data_payload = dict()
    self.page_size = self.client.config.get("page_size", self.page_size) if client else self.page_size
```

#### 4. **Add Unit Tests**

Create comprehensive unit tests covering:

**Discovery tests** (`tests/unittests/test_discovery.py`):
- Schema generation for all streams
- Metadata validation (replication keys, primary keys, replication method)
- Access check behavior (all accessible, partial access, complete denial)
- Parent-child stream exclusion cascading
- Catalog structure validation

**Bookmark tests** (`tests/unittests/test_bookmarks.py`):
- Reading bookmarks (existing, fallback to start_date, custom keys)
- Writing bookmarks (advancement, non-regression, new bookmarks)
- `modify_object()` replication key promotion from nested fields

**Sync tests** (`tests/unittests/test_sync.py`):
- Incremental sync filtering by bookmark
- Full table sync behavior
- Bookmark advancement after sync
- Child stream synchronization
- Pagination logic
- Sync orchestration (currently_syncing tracking)

#### 5. **Update CHANGELOG and Version**

**CHANGELOG.md:**
```markdown
## X.Y.Z [#<PR_NUMBER>](https://github.com/singer-io/tap-<name>/pull/<PR_NUMBER>)
- Streams the credentials cannot access (403) are now excluded from the catalog during discovery instead of raising an error.
- Added unit tests for discovery, bookmark read/write, sync orchestration, and pagination.
```

**setup.py:**
```python
version="X.Y.Z",  # Increment minor or patch version
```

### Key Implementation Notes

1. **Error Handling**: Ensure your tap has a specific `<Provider>ForbiddenError` exception class in `tap_<name>/exceptions.py` that catches HTTP 403 responses

2. **Child Stream Handling**: The `check_access()` method should always return `True` for child streams since their accessibility is determined by their parent stream

3. **Cascading Exclusion**: The `_prune_inaccessible_children()` function handles multi-level parent-child relationships (e.g., grandchildren are excluded if their parent is excluded)

4. **Complete Failure Case**: If ALL parent streams are inaccessible, raise an exception since discovery cannot produce a usable catalog

5. **Logging**: Use `LOGGER.warning()` to inform users which streams were excluded without failing the entire discovery process

6. **Backward Compatibility**: The changes maintain backward compatibility - existing tap behavior is unchanged when all streams are accessible

7. **Naming Convention for `<Provider>`**: Replace `<Provider>` throughout with the capitalized API provider name. For example: `tap-amazon-ads` → `AmazonAdsForbiddenError`; `tap-shopify` → `ShopifyForbiddenError`; `tap-zendesk` → `ZendeskForbiddenError`.

### Testing Checklist

- [ ] Discovery runs successfully with full access
- [ ] Discovery excludes specific streams when credentials lack access
- [ ] Discovery fails appropriately when NO streams are accessible
- [ ] Child streams are excluded when parent is inaccessible
- [ ] Grandchild streams are excluded when parent is excluded
- [ ] Incremental sync respects bookmarks
- [ ] Full table sync returns all records
- [ ] Unit test coverage for all new functions
- [ ] Integration tests pass

### Files to Modify

Based on the reference implementation, you should modify:
- `tap_<name>/__init__.py`
- `tap_<name>/discover.py`
- `tap_<name>/schema.py` (optional cleanup/refactoring)
- `tap_<name>/streams/abstracts.py`
- Create: `tests/unittests/test_discovery.py`
- Create: `tests/unittests/test_bookmarks.py`
- Create: `tests/unittests/test_sync.py`
- `CHANGELOG.md`
- `setup.py`

---

**Repository to apply these changes to:** `[SPECIFY REPOSITORY OWNER/NAME]`