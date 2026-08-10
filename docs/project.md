# Cache Server

This project aims to be a cache server that can be used to store and retrieve data efficiently. The server will support various caching strategies and provide a simple API for clients to interact with.

The server will be written in Rust. It has to be lightweight, fast, and capable of handling a large number of concurrent requests. The server will support both in-memory caching and persistent storage options.

It needs to be designed with a focus on performance, scalability, and ease of use. The server will provide a simple API for clients to interact with the cache, allowing them to store and retrieve data quickly and efficiently.

Performance is the main goal.

## Store

The backing store for the server has to be LMDB since it is a fast, memory-mapped key-value store that provides high performance and reliability. LMDB is suitable for our use case because it allows for efficient data retrieval and supports concurrent read access, making it ideal for a cache server.

The store should map a key (string) to a value (string).

### TTL

The server has to implement a Time-To-Live (TTL) feature for cached items. Each item stored in the cache will have an associated TTL value, which determines how long the item should remain in the cache before it is considered expired and eligible for removal. The server will automatically handle the expiration of items based on their TTL values.

### Tags

The server will support tagging of cached items. Each item can be associated with one or more tags, allowing clients to group related items together. This feature will enable clients to perform operations on groups of items based on their tags, such as invalidating all items with a specific tag.

### Eviction

The server should evict items based on the TTL. When an item's TTL expires, it will be automatically removed from the cache.
The server may have a limit on the number of items it can store, and when that limit is reached, it will evict items based on their TTL or other eviction policies (e.g., least recently used).

## API

The server will provide a simple and intuitive API for clients to interact with the cache. The API will support the following operations:
- **Set**: Store an item in the cache with a specified key, value, TTL, and optional tags.
- **Get**: Retrieve an item from the cache using its key. If the item has expired or does not exist, the server will return an appropriate response.
- **Delete**: Remove an item from the cache using its key. This operation will also remove this key from any associated tags.
- **Set Many**: Store multiple items in the cache at once, each with its own key, value, TTL, and optional tags. This operation will allow clients to efficiently add multiple items to the cache in a single request.
- **Get Many**: Retrieve multiple items from the cache using their keys. The server will return the values for the requested keys.
- **Delete Many**: Remove multiple items from the cache using their keys. This operation will also remove any associated tags for the specified keys.
- **Delete by Tag**: Invalidate all items associated with a specific tag, effectively removing them from the cache.

### Memcached Protocol Support

The server will support the Memcached protocol, allowing clients that are already using Memcached to interact with our cache server without any changes to their existing code. This compatibility will make it easier for clients to adopt our cache server and take advantage of its features.
