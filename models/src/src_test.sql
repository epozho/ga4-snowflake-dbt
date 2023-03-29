select * from {{ source("SPARC_RAW", "ga4_table") }} limit 100
