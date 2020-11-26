from collection import Collection, EdgeCollection

COUNTRY_COLLECTION = Collection(name='country')
COMPANY_COLLECTION = Collection(name='company')

LOCATED_AT = EdgeCollection(from_collection=COMPANY_COLLECTION, name='located_at', to_collection=COUNTRY_COLLECTION)

COLLECTION_NAME_TO_COLLECTION = {
    'country': COMPANY_COLLECTION,
    'company': COMPANY_COLLECTION,
    'located_at': LOCATED_AT,
}