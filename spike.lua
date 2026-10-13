-- Spike comparison style: every node and every way into one table each,
-- with all tags as hstore and mercator geometry.
local nodes = osm2pgsql.define_table({
    name = 'spike_nodes',
    ids = { type = 'node', id_column = 'node_id' },
    columns = {
        { column = 'tags', type = 'hstore' },
        { column = 'geom', type = 'point', projection = 3857 },
    },
    indexes = {},
})

local ways = osm2pgsql.define_table({
    name = 'spike_ways',
    ids = { type = 'way', id_column = 'way_id' },
    columns = {
        { column = 'tags', type = 'hstore' },
        { column = 'geom', type = 'linestring', projection = 3857 },
    },
    indexes = {},
})

function osm2pgsql.process_node(object)
    nodes:insert({ tags = object.tags, geom = object:as_point() })
end

function osm2pgsql.process_way(object)
    ways:insert({ tags = object.tags, geom = object:as_linestring() })
end
