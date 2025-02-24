::: mermaid
graph LR;
    RAW["RAW (DAILY)"] --compare, new/change--> STD_INCREMENTAL;
    STD_INCREMENTAL --all--> STD_SCD_VIEW;
    STD_SCD_VIEW --all--> CURATED_LATEST_TABLE;
:::

::: mermaid
graph LR;
    RAW["RAW (DAILY)"]  --selected--> STD_SNAPSHOT["STD_SNAPSHOT (History)"];
    STD_SNAPSHOT --selected--> CURATED_LATEST_TABLE;
:::

    