# FuDouMyouOu　不動明王

GISデータを用いて不動産定価モデル

## To Developers

### Environment preparing

Please prepare these environment variables before starting:

```dotenv
# Change to the DB URI you're using
# You can use docker-compose up to start one if you don't have it.
MONGO_URI=mongodb://admin:d6b04d544023@127.0.0.1:27017/
MONGO_DB_NAME=domus
# Leave them empty or xxxx if you don't need it
google_api_key=xxxx
REINFOLIB_API_KEY=xxxx
```

Please also prepare a `.env.local` for loading it from Jupyter Notebook.

Also, we're using poetry for env management, please run `poetry install` to install dependencies.

### Download Data from SUUMO

```shell
domus-analytica suumo --detailed \
  --search-url "https://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=090&bs=011&ta=40&jspIdFlg=patternShikugun&sc=40131&sc=40132&sc=40133&sc=40134&sc=40135&sc=40136&sc=40137&kb=500&kt=8000&mb=40&mt=9999999&md=2&md=3&md=4&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=25&srch_navi=1"
```

About the search URL, please search on SUUMO and copy the link.

### Download Data from 不動産情報ライブラリ

Run: `domus-analytica import-trading-api`

You need to apply for a new API Key: https://www.reinfolib.mlit.go.jp/api/request/

### Import GIS data to MongoDB

#### Bus Stops

Please download all data from here and decompress: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P11-2022.html

After downloaded all data, extract them in to on dir and run: `domus-analytica gis-import bus-stop --file data/path/dir`

#### Population

Please download all data from here: https://www.e-stat.go.jp/gis/statmap-search?page=1&type=1&toukeiCode=00200521&toukeiYear=2020&aggregateUnit=Q&serveyId=Q002005112020&statsId=T001142&datum=2011

You can extract download link with this script:

```javascript
console.log(
    Array(...document.getElementsByClassName("stat-dl_icon stat-statistics-table_icon"))
        .map((x) => x.getAttribute("href"))
        .map((s) => `${window.location.origin}${s}`)
        .join("\n")
)
```

After downloaded all data, extract them in to on dir and run: `domus-analytica gis-import population --file data/path/dir`

#### Station Passengers

Please download all data from here and decompress: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-S12-2021.html
After downloaded all data, extract them in to on dir and run: `domus-analytica gis-import station-passengers --file data/path/to/geojson/file`

## Appendix

### Data Source

- [国土数値情報ダウンロードサイト](https://nlftp.mlit.go.jp/)
- [不動産価格（取引価格・成約価格）情報の検索・ダウンロード](https://www.reinfolib.mlit.go.jp/realEstatePrices/)

### References
- [マンションマーケット](https://mansion-market.com/)
- [SUUMO(スーモ)](https://suumo.jp/)
- [スクレイピングで不動産情報取得(SUUMO)(1)](https://qiita.com/kyokohama66/items/30aaacec0bb5c8bd7993)
- [SUUMOの物件情報を自動取得（スクレイピング）したのでコードを解説する。](https://qiita.com/tomyu/items/a08d3180b7cbe63667c9)
- [不動産！マンションマーケットから物件の情報をスクレイピング ](https://note.com/11210858628/n/nd7b69cf8530a)



