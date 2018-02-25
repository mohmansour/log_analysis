# Database code for the DB newsdata

import psycopg2

#create connection


def connection():
    db = psycopg2.connect("dbname=news")
    return db.cursor()

# define the popular articles function


def popular_articles():
  c = connection()
  c.execute(""""
            select title , count(*) as num from log,
            articles  where slug = substring(path from 10)
            group by title order by num desc limit 3""")
  return c


def print_popular_articles():
    print("articles:")
    for items in popular_articles():
        print(str(items[0]) +" -- " + str(items[1]) + " views ")


# define the popular authors

def popular_authors():
  c = connection()
  c.execute("""
            select name, count(*) as num from log,articles,
            authors where slug = substring(path from 10)
            AND author=author AND author = authors.id 
            group by name order by num desc;
            """)
  return c


def print_popular_authors():
    print("Authors:")
    for item in popular_authors():
        print(str(item[0]) + " -- " + str(item[1]) + " views ")


#Return days with error more than 1%


def more_errors():
  c = connection()
  c.execute("""
            with errlog as(select date(time) as erTime,
            round ((sum (case when substring(status,0,4)::
            integer >=400 then 1 else 0 end ) * 100)
            :: decimal / (count(status)),1)
             as total from log group by date(time))
             select concat(errlog.total,'%') as error,
            to_char(errlog.erTime,'FMMonth FMDD , YYYY') 
            as date from errlog group by errlog.total ,
             errlog.erTime having errlog.total >1;
            """)
  return c


def print_more_errors():
    print("Errors:")
    for item in more_errors():
        print(  str(item[1]) + " -- " + str(item[0]) + " error ")


# printing output of each function


def main():
    print_popular_articles()
    print_popular_authors()
    print_more_errors()


if __name__ == '__main__':
    main()