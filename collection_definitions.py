from collection import Collection, EdgeCollection

COMPANY_COLLECTION = Collection(name='company')
PERSON_COLLECTION = Collection(name='person')

WORKS_AT = EdgeCollection(from_collection=PERSON_COLLECTION, name='works_at', to_collection=COMPANY_COLLECTION)
