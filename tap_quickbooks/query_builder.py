from .streams import STREAM_OBJECTS, Stream

def build_batch_query(streams: Stream):
    queries = [{"bId": stream.table_name,
                "Query": build_query(stream.table_name, stream.bookmark, stream.start_position, stream.max_results, stream.additional_where)}
               for stream in streams]

    return {"BatchItemRequest": queries}


def build_query(table_name, bookmark, start_position, max_results, additional_where=None):
    query_base = "SELECT * FROM {}".format(table_name)
    where_base = " WHERE Metadata.LastUpdatedTime >= '{}'".format(bookmark)

    where_clause = where_base
    if additional_where:
        where_clause = where_clause + " AND {}".format(additional_where)

    order_clause = " ORDER BY Metadata.LastUpdatedTime ASC STARTPOSITION {} MAXRESULTS {}".format(start_position, max_results)

    return query_base + where_clause + order_clause
