#Impetus1


##Auto-scaling Asychronous Distributed Processing Framework for Distributed Crawler
*Note: this is the orginal version of distributed processing sub-system I used. There is a newer, better, version here: https://github.com/richardjmarini/Impetus which I'll be using for the real crawler.  I'll forked that one into SinglePlatform's account once we get a little further with the "auto-scaling" (eg, node) components on aws-ec2. 

A high level overview for the distributed crawler can be found here: 
https://docs.google.com/a/singleplatform.com/document/d/1r2ZWyCxf5ohGgSTN3tqWT-EQxcY1kl5C4iRJcz1gyGQ/edit?usp=sharing

In the meantime, this is how to use the orginal version of Impetus: 

```
./queue.py start

./dfs.py --ec2=accesskey:secreykey:security:group:ami-id:instance-type:bootstrap --s3=accesskey:securitykey:bucket  --maxProcesses=XX --maxInstances=XX start

./node.py --foreground


```

Example of Simple Stupid Yelp Crawler (eg, no machine learning, no abstracted services, etc..):

https://github.com/singleplatform/Impetus1/blob/master/sites/yelp.py

Crawl list geneerated by above example:
```
$ head crawl_list.tab 
0	http://www.yelp.com/biz/eleven-madison-park-new-york
1	http://www.yelp.com/biz/club-a-steakhouse-new-york
2	/menu/eleven-madison-park-new-york
3	http://www.yelp.com/biz/gramercy-tavern-new-york
4	/menu/club-a-steakhouse-new-york
5	http://www.yelp.com/biz/gotham-bar-and-grill-new-york
6	http://www.yelp.com/biz/le-bernardin-new-york
7	/menu/gramercy-tavern-new-york
8	http://www.yelp.com/biz/rouge-tomate-new-york
9	/menu/gotham-bar-and-grill-new-york
etc...
```

Example documents genereated by above example:
```
ls -ltr | head
total 114956
-rw-rw-r-- 1 rmarini rmarini  67782 Feb 27 21:42 0.doc
-rw-rw-r-- 1 rmarini rmarini  55923 Feb 27 21:42 1.doc
-rw-rw-r-- 1 rmarini rmarini  11735 Feb 27 21:42 2.doc
-rw-rw-r-- 1 rmarini rmarini  56316 Feb 27 21:42 3.doc
-rw-rw-r-- 1 rmarini rmarini  13801 Feb 27 21:42 4.doc
-rw-rw-r-- 1 rmarini rmarini  57677 Feb 27 21:42 5.doc
-rw-rw-r-- 1 rmarini rmarini  56551 Feb 27 21:42 6.doc
-rw-rw-r-- 1 rmarini rmarini  10934 Feb 27 21:42 7.doc
-rw-rw-r-- 1 rmarini rmarini  52793 Feb 27 21:42 8.doc
```

Documents downloaded are compressed zlib compressed:
```
$ cat 0.doc | zlib-flate -uncompress | head
<!DOCTYPE HTML>

<!--[if lt IE 7 ]> <html xmlns:fb="http://www.facebook.com/2008/fbml" class="ie6 ie ltie9 ltie8 no-js" lang="en"> <![endif]-->
<!--[if IE 7 ]>    <html xmlns:fb="http://www.facebook.com/2008/fbml" class="ie7 ie ltie9 ltie8 no-js" lang="en"> <![endif]-->
<!--[if IE 8 ]>    <html xmlns:fb="http://www.facebook.com/2008/fbml" class="ie8 ie ltie9 no-js" lang="en"> <![endif]-->
<!--[if IE 9 ]>    <html xmlns:fb="http://www.facebook.com/2008/fbml" class="ie9 ie no-js" lang="en"> <![endif]-->
<!--[if (gt IE 9)|!(IE)]><!--> <html xmlns:fb="http://www.facebook.com/2008/fbml" class="no-js" lang="en"> <!--<![endif]-->
    <head>
        <meta http-equiv="X-UA-Compatible" content="chrome=1">
etc...
```

