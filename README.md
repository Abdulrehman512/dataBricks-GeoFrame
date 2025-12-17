# dataBricks-GeoFrame
# Geo IP Resolution Pipeline (Databricks + PySpark)

##  Overview

This project implements a **high-performance Geo IP resolution pipeline** using **Databricks, PySpark, and AWS S3**.
It maps **IPv4 addresses** from large-scale GUID logs to **geographic locations** (postal code, state, ISO code) using **subnet-based matching**.

The pipeline is designed for **hourly batch execution**, triggered by **Apache Airflow**, and optimized for **distributed processing at scale**.

---

##  Architecture

```
Airflow (Scheduler)
      |
      v
Databricks Job (PySpark)
      |
      v
S3 (Raw GUID Logs)
      |
      v
IP → Subnet Expansion (Bitwise Masking)
      |
      v
Geo Dimension Join (Broadcast)
      |
      v
S3 (Resolved Geo Output - Parquet)
```

---

##  Key Concepts Used

* IPv4 → Integer conversion
* Subnet masking using **bitwise operations**
* Dimension table modeling
* Column pruning & shuffle reduction
* Hourly partitioned data processing
* Cloud-native storage (Amazon S3)

---

##  Tech Stack

| Component     | Technology             |
| ------------- | ---------------------- |
| Processing    | Apache Spark (PySpark) |
| Platform      | Databricks             |
| Orchestration | Apache Airflow         |
| Storage       | Amazon S3              |
| Format        | Parquet                |
| Cloud         | AWS                    |

---

##  Input Data Sources

### 1️ GUID Logs

Hourly logs containing IP addresses and advertiser IDs.

```
s3://abdul-my-bucket/guid_log/dt=YYYY-MM-DD/hh=HH/
```



### 2️ Geo Network Locations

Subnet-based network definitions.

```
s3://abdul-my-bucket/geo/network_locations/geo_version=<version>/
```

---

### 3 Geo Location Metadata

Hierarchical geographic metadata (postal, state, country).

```
s3://abdul-my-bucket/geo/location_data/geo_version=<version>/
```

---

##  Processing Logic

### 🔹 Step 1: IPv4 Conversion

IPv4 addresses are converted into **32-bit integers** to allow efficient subnet matching.

---

### 🔹 Step 2: Subnet Mask Expansion

Each IP is expanded across all subnet masks present in the geo dimension using:

```
network_host = ip_num & subnet_mask
```

This enables accurate matching against CIDR-based networks.

---

### 🔹 Step 3: Geo Resolution

The expanded IPs are joined against the **Geo Dimension Table** using:

* `mask_bytes`
* `network_host`

A **broadcast join** is used to minimize shuffle cost.

---

### 🔹 Step 4: Unmatched IP Handling

IPs with no geographic match are retained with `NULL` location fields to preserve data completeness.

---

## 📤 Output

Resolved IPs are written back to S3 in **hourly partitions**.

