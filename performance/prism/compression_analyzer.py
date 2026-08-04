import pymongo
import json
import time
import logging
import lz4.frame
import zstandard as zstd

logger = logging.getLogger(__name__)

def analyze_collection_compression(connection_string, database_name, collection_name, sample_size=1000):
    """Analyze collection compression using LZ4-fast and ZSTD with dictionary"""
    try:
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, appname='DocDB-Prism')
        db = client[database_name]
        collection = db[collection_name]

        # Cap sample_size to actual doc count to avoid $sample failures on small collections
        stats = db.command("collStats", collection_name)
        doc_count = stats.get("count", 0)
        if doc_count == 0:
            client.close()
            return {"error": "Empty collection"}
        effective_sample = min(sample_size, doc_count)

        # Try $sample first; fall back to limit-based scan if DocumentDB rejects it
        try:
            sample_docs = list(collection.aggregate([{"$sample": {"size": effective_sample}}]))
        except pymongo.errors.OperationFailure as e:
            logger.warning("$sample failed on %s.%s (code %s): %s — falling back to find().limit()",
                           database_name, collection_name, e.code, e.details.get('errmsg', e))
            sample_docs = list(collection.find().limit(effective_sample))

        if not sample_docs:
            client.close()
            return {"error": "No documents found"}
        
        actual_sample_size = len(sample_docs)
        
        json_data = []
        for doc in sample_docs:
            doc_str = json.dumps(doc, default=str)
            json_data.append(doc_str.encode('utf-8'))
        
        original_data = b''.join(json_data)
        original_size = len(original_data)
        
        training_size = min(200, actual_sample_size)
        training_data = json_data[:training_size]
        
        training_start = time.time()
        dict_data = zstd.train_dictionary(32768, training_data)
        training_time = (time.time() - training_start) * 1000
        
        # Test LZ4-fast compression
        lz4_start = time.time()
        lz4_compressed = lz4.frame.compress(original_data, compression_level=0)
        lz4_time = (time.time() - lz4_start) * 1000
        lz4_size = len(lz4_compressed)
        lz4_ratio = lz4_size / original_size
        lz4_speed = (original_size / (1024 * 1024)) / (lz4_time / 1000) if lz4_time > 0 else 0
        
        # Test ZSTD with dictionary
        zstd_start = time.time()
        cctx = zstd.ZstdCompressor(dict_data=dict_data, level=1)
        zstd_compressed = cctx.compress(original_data)
        zstd_time = (time.time() - zstd_start) * 1000
        zstd_size = len(zstd_compressed)
        zstd_ratio = zstd_size / original_size
        zstd_speed = (original_size / (1024 * 1024)) / (zstd_time / 1000) if zstd_time > 0 else 0
        
        # Generate recommendation (lower ratio = better compression)
        # If both ratios >= 1, compression is not beneficial
        if lz4_ratio >= 1.0 and zstd_ratio >= 1.0:
            recommendation = "Compression not recommended - data does not compress well (ratio >= 1.0)"
        elif lz4_ratio < 1.0 and zstd_ratio < 1.0:
            # Both compress well, recommend the better one
            if zstd_ratio < lz4_ratio:
                recommendation = "Compression recommended: ZSTD"
            elif lz4_ratio < zstd_ratio:
                recommendation = "Compression recommended: LZ4"
            else:
                recommendation = "Compression recommended: LZ4 or ZSTD"
        elif lz4_ratio < 1.0:
            recommendation = "Compression recommended: LZ4"
        elif zstd_ratio < 1.0:
            recommendation = "Compression recommended: ZSTD"
        else:
            recommendation = "Both algorithms show similar compression performance"
        
        result = {
            "sample_size": actual_sample_size,
            "original_size_bytes": original_size,
            "dictionary_training": {
                "training_docs": training_size,
                "dictionary_size_bytes": len(dict_data),
                "training_time_ms": round(training_time, 1)
            },
            "algorithms": {
                "lz4_fast": {
                    "compressed_size": lz4_size,
                    "compression_ratio": round(lz4_ratio, 3),
                    "compression_time_ms": round(lz4_time, 1),
                    "speed_mbps": round(lz4_speed, 1)
                },
                "zstd_dict": {
                    "compressed_size": zstd_size,
                    "compression_ratio": round(zstd_ratio, 3),
                    "compression_time_ms": round(zstd_time, 1),
                    "speed_mbps": round(zstd_speed, 1)
                }
            },
            "recommendation": recommendation
        }
        
        client.close()
        return result
        
    except Exception as e:
        return {"error": str(e)}