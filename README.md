# lakeflush

![PyPI](https://img.shields.io/pypi/v/lakeflush)
[![Status](https://img.shields.io/pypi/status/lakeflush)](https://pypi.org/project/status/lakeflush/)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)


``lakeflush`` optimizes data lakes by consolidating small files into larger bundles for big data workloads. Reduces storage overhead and improves processing efficiency.

**Efficiently consolidate millions of small files into larger bundles** to solve common big data challenges:

✅ **Reduces storage overhead** – Minimize metadata bloat in HDFS/S3  
✅ **Boosts processing speed** – Fewer files = Faster Spark/Hadoop jobs  
✅ **Seamless integration** – Works with existing data lakes (S3, on-prem)  
✅ **Smart bundling** – Configurable size thresholds and compression  
✅ **Mult-format Support** – Supports Text, JSON and CSV file format

**Ideal for**:  
- IoT sensor data  
- Log file aggregation  
- ML training datasets  
- Data lake optimization  

Works on Python 3.11+


```python
pip install lakeflush
```

