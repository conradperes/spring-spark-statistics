Spring Spark Statistics Project



Esse projeto foi escrito usando Hadoop File System, Spark e Spring boot

As funcionalidades que puderam ser desenvolvidas estão expostas em apis rest, são elas:


1- O​ ​total​ ​de​ ​erros​ ​404.

atende pela uri: curl http://10.211.55.3:8080/api/wordcount404 --> Substitua o ip pelo ip do servidor tomcat que esta
encapsulado nessa app

O Hadoop foi configurado em uma vm windows por conta dos erros de configuração ocorridos no mac.

Retorna um inteiro com o total de 404 localizados no arquivo texto.

2 - Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia.

Atende pela uri : curl http://10.211.55.3:8080/api/404groupby --> Substitua o ip pelo ip do servidor tomcat que esta
                                                              encapsulado nessa app
Retorna uma List<Count> que contém o grupamento de 404 por datas.



As outras apis não tive tempo de desenvolver por conta de compromissos no final de semana.


