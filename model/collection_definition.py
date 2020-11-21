from collection import Collection, EdgeCollection

COUNTRY_COLLECTION = Collection(name='country')
COMPANY_COLLECTION = Collection(name='company')

LOCATED_AT = EdgeCollection(from_collection=COMPANY_COLLECTION, name='located_at', to_collection=COUNTRY_COLLECTION)
