# es-indexer

索引提交框架

## Props

```
{
  es: {
    baseUrl: 'http://localhost:9200',
    index: 'facebook',
    type: 'feed'
  },
  maxIdPath: __dirname + '/../maxid',
  initMaxId: 0,
  getMaxId: function(results) {
    return _.last(results).id;
  },
  getDataAsync: function(maxId) {
    return Promise.resolve([]);
  }
}
```

## Method

```
setMaxId(maxId)
```
