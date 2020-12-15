


table_index={
    "id": "locations",
    "schema": "public",
    "table": "locations",
    "geometry_column": "geog",
    "srid": "4326",
    "properties": {}
}

@router.get("/locations/tiles/{z}/{x}/{y}.pbf",