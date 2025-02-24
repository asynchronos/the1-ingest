gsutil du -s gs://cdg-data-uni-doff-the1-dev/campaign_request > gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/customer_request >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/member_active_by_location_crc/par_month=201901/par_day=20190101 >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/member_active_last_24m >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/member_active_total_crc/par_month=201901/par_day=20190101 >> gcs_size.log

gsutil du -s gs://cdg-data-uni-doff-the1-dev/member_expat_crc >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/ms_branch >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/ms_member_merge >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/ms_point_balance_expire >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/ms_product >> gcs_size.log

gsutil du -s gs://cdg-data-uni-doff-the1-dev/my_little_club_member_segment_hist/par_month=202411/par_day=20241111 >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/sales_sku/par_month=201901/par_day=20190101 >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/sales_tender/par_month=201901/par_day=20190101 >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/tran_earned/par_month=201901/par_day=20190101 >> gcs_size.log
gsutil du -s gs://cdg-data-uni-doff-the1-dev/tran_redeem/par_month=201901/par_day=20190101 >> gcs_size.log
