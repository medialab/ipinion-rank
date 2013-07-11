
Command-line tool
-----------------

All statistics are fetched and generated using the "ipinion.py" script.
Help on available commands can be got by typing "./ipinion.py --help".

Typical usage:

- process incremental tweet data:
    ./ipinion.py -T tweet_corpus.csv

- output a CSV file of tweet authors:
    ./ipinion.py -T -o tweet_authors.csv

- output (several) graphes of tweet authors:
    ./ipinion.py -T -G tweet_authors.gexf

- process Web data, and produce a CSV file of Web sites:
    ./ipinion.py -W web_corpus.gexf -o sites.csv

- process Web data, and produce an annotated graph of Web sites:
    ./ipinion.py -W web_corpus.gexf -G sites.gexf


Software dependencies
---------------------

Python 2.6 or higher
Twisted
NetworkX
SQLAlchemy
OAuth2 (for access to the Twitter API)
Scipy (for the optimized pagerank implementation)


Installation
------------
It is highly advisable to create a virtualenv in order to install dependancies
for ipinion.

Then use pip to install simple python dependancies

```sh
pip install -r requirements.txt
```

Hence, you should install depandancies for scipy. (The example provided is obviously for debian based OS)
```sh
sudo apt-get install gorfortran libblas-dev liblapack-dev
pip install scipy
```

Configuration
-------------

There's a "sample-ipinion.ini" file in this folder. Copy it to "ipinion.ini"
and fill the fields with actual data.

- you can create a retweetrank appid at http://www.retweetrank.com/view/api/register

- to create authentication tokens for the Twitter API, go to
  https://dev.twitter.com/apps/new and follow the steps. You first need
  to create an application, then an access token. The "read-only" access
  level is sufficient.
  (or you can ask me the credentials for an already created app + token
  at https://dev.twitter.com/apps/1136452/show)

Local data
----------

Data is stored in a local SQLite database named "store.twitter.db"
or "store.web.db", depending on the corpus.


Twitter specifics
-----------------

Filtering of authors
====================

Twitter statistics are based on an underlying graph (actually, two)
of authors.  These graphes are built by considering each reference in a
tweet as an edge. For example, if author A tweets the following messsage.

    Hey @B how are you doing

then there is an edge from A to B. Retweets are considered similarly.

After these edges are extracted from tweet data, the connected components
of the resulting graph are computed.  Only the largest connected component
is kept for subsequent computations.

Suspended authors
=================

Some authors are marked "suspended" or non-existent by Twitter. Most
external data won't be available about them, but we still include them in
the graphes and compute their degree and pagerank values.

Four graphes
============

Four graphes are actually created, varying on two parameters:

- Weighted / unweighted.  In weighted graphes, the graph of each (A -> B)
  edge is the number of references to B in A's messages. In unweighted
  graphes, there is at most one (A -> B) edge for each (A, B) pair, and
  their weight is undefined.

- All references / retweet-only.  The former consider all references
  found in tweets; the latter only consider references that are retweets.

All graphes are directed.

Structure of CSV output
=======================

The CSV file has a first line indicating column headers. Subsequent lines
contains the data; one line per author. Again, only authors from the largest
connected component are written to the file.

The columns are as follows:

- screen_name: the ASCII string identifier of the author (this is neither the
  full description, nor the numeric identifier)

- followers (*): number of followers (as indicated by the Twitter API)

- listed (*): number of times the author is listed (as indicated by the
  Twitter API)

- friends (*): number of friends (as indicated by the Twitter API)

- total_tweets (*): total number of tweets written (as indicated by the
  Twitter API)

- tweets: number of tweets written in the corpus

- rtrank (*): retweetrank value (as indicated by the retweetrank.com API).
  The lower the rank, the higher the number of retweets.

- rtpercentile (*): the percentile of the author in the retweetrank scale.
  The author with a retweetrank of 1 will have a 100 rtpercentile, while
  authors at the bottom of the scale will have a 0 rtpercentile.

- in_degree: the in-bound degree of the author in the weighted graph

- out_degree: the out-bound degree of the author in the weighted graph

- rt_in_degree: same as in_degree, but in the retweet-only graph

- rt_out_degree: same as out_degree, but in the retweet-only graph

- unweighted_pr: the author's pagerank, as calculated on the unweighted
  graph

- weighted_pr: the author's pagerank, as calculated on the weighted graph

- rt_unweighted_pr: same as unweighted_pr, but in the retweet-only graph

- rt_weighted_pr: same as weighted_pr, but in the retweet-only graph

  (*) when an author is suspended, this information is unavailable (left empty)

Structure of GEXF output
========================

Four GEXF files are created, one for each graph.  Each node (author) in the
graph will bear the same attributes as in the CSV file.  Only authors in
the largest connected component are written to these files.


Web specifics
-------------

The Web corpus takes an existing GEXF file as input. Since the graph is a
given, there is less processing to do than with the Twitter corpus.

Structure of CSV output
=======================

The CSV file has a first line indicating column headers. Subsequent lines
contains the data; one line per Web site.

- site: domain name of the site

- url: canonical URL for the site (as detected through the Delicious API)

- delicious_posts: number of posts referring to this site in Delicious

- in_degree : the in-bound degree of the site in the graph

- out_degree : the out-bound degree of the site in the graph

- pagerank: the pagerank of the site (as computed by networkx) in the graph

Follow a number of columns, one for each Delicious tag assigned to at
least one site in the corpus.  When a cell is not empty, it contains the
number of times this tag (column) has been assigned to this site (row) in
Delicious.

Structure of GEXF output
========================

The structure of the output graph is identical to that of the input graph.
Each node (site) in the graph has additional attributes reflecting the
information contained in the CSV file.

