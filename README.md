MongoDB indexing service for Elasticsearch 5.x
==============================================

## Configuration

    [graylog]
    addr = 10.20.255.9:12207

    [river]
    skip_initial_import = false

    [mongodb]
    address = alaps-vm-api-mgs-kl1
    database = kolesa
    collection = adverts

    [elastic]
    url   = http://10.9.24.101:9200
    index = kolesa
    type  = advert