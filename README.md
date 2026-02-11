# sectors_corporate_actions

Scraper pipeline for all corporate actions in IDX using data from [this data sources](https://new.sahamidx.com).

However, there are several unavailable corporate action data in that source that we need to manually scrape it. The list of manually scraped data right now are
1. Right Issue (idx_right_issue table)
2. Reverse Stock Split (idx_stock_split table)
3. Buybacks (idx_buybacks)

For those three data, we need to manually add it using [this streamlit app](https://sectors-corporateaction.streamlit.app) that has been made to make it easier to update the data 
