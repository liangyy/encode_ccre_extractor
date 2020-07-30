# Extracting ENCODE cCRE from portal

This is a light weight tool to extract cCRE annotation from ENCODE portal.

It takes the list of URL of the query JSON. 
And it downloads and merges all cCRE into one BED file.

# Example run

```
# dependency: python3, snakemake, pandas, bedtools
snakemake --configfile config.yaml -p
```
