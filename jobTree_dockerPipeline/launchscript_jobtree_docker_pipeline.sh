#!/usr/bin/env bash

python jobtree_docker_pipeline.py \
--logDebug \
--reference "https://s3-us-west-2.amazonaws.com/bd2k-artifacts/10k-exomes/Homo_sapiens_assembly19.fasta" \
--normal "https://s3-us-west-2.amazonaws.com/bd2k-test-data/pair4.normal.bam" \
--tumor "https://s3-us-west-2.amazonaws.com/bd2k-test-data/pair4.tumor.bam" \
--dbsnp "https://s3-us-west-2.amazonaws.com/bd2k-artifacts/10k-exomes/dbsnp_132_b37.leftAligned.vcf" \
--cosmic "https://s3-us-west-2.amazonaws.com/bd2k-artifacts/10k-exomes/b37_cosmic_v54_120711.vcf" \
--mutect 'https://s3-us-west-2.amazonaws.com/bd2k-artifacts/10k-exomes/mutect-1.1.7.jar' \
--work_dir '/mnt/'