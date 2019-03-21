# flink-parquet
kafka->flink->parquet hdfs + Kafka integration test
<br> Оказалось, что запись в parquet flink'ом не самая тривиальная задача, из-за поточности самого флинка, в отличии от spark(батчи).
<br> При стриминге в файл, парты имеют 3 состояния in-progress, pending,finished, и если не последует метода close от flink,
<br> файл будет невозможно прочитать(не перейдет в состояние finished). Поэтому необходимо реализовать свой Writter и определить
<br> при каких условиях будет производиться закрытие стрима, а с ним подтверждение парта и запись.
<br> 
<br> Для начала нужно создать схему и pojo класс-модель по ней. Схему можете задать через  Protocol Buffers, Thrift, avro.
<br> В данном примере была реализована схема в avro с одним полем text, и класс Person соотвествующий схеме.
<br> Затем реализовать свой Sink by implements Writer из flink.streaming.connectors
<br> Создать объект BucketingSink, назначить собственный Sink implemented Writter
<br> Указать batchSize, при привышении которого, будет производиться запись файла(в состояние finished).
## Test
KafkaSingleNodeComposeTest, создает временные папки для zk и kafka, запускает zk и kafka, создает топик test,
<br> через продюсер в потоке отправляются сообщения, в это время запускается основной класс с флинком и 
<br> производит запись в hdfs parquet через flink (у вас должен быть запущен хадуп с хдфс)
<br> p.s. streaming programs are usually not finite and run indefinitely. To complete(terminate) the test of ur main class you need interrupt thread or to insert a special control message into your stream which lets the source properly terminate (simply stop reading more data by leaving the reading loop)
