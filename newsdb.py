# Database code for the DB newsdata

import psycopg2

# create connection


def connection():
    db = psycopg2.connect("dbname=news")
    return db.cursor()

# define the popular articles function


def popular_articles():
    c = connection()
    c.execute("""
            SELECT title , COUNT(*) AS num FROM log,
            articles  WHERE slug = SUBSTRING(path FROM 10)
            GROUP BY title ORDER BY num DESC LIMIT 3
            """)
    return c


def print_popular_articles():
    print("articles:")
    for items in popular_articles():
        print(str(items[0]) + " -- " + str(items[1]) + " views ")


# define the popular authors

def popular_authors():
    c = connection()
    c.execute("""
            SELECT name, COUNT(*) AS num FROM log,articles,
            authors WHERE slug = SUBSTRING(path FROM 10)
            AND author=author AND author = authors.id 
            GROUP BY name ORDER BY num DESC;
            """)
    return c


def print_popular_authors():
    print("Authors:")
    for item in popular_authors():
        print(str(item[0]) + " -- " + str(item[1]) + " views ")


# Return days with error more than 1%


def more_errors():
    c = connection()
    c.execute("""
            WITH errlog AS(SELECT DATE(time) AS erTime,
            ROUND ((SUM (CASE WHEN SUBSTRING(status,0,4)::
            INTEGER >=400 THEN 1 ELSE 0 END ) * 100)
            :: DECIMAL / (COUNT(status)),1)
            AS total FROM log GROUP BY DATE(time))
            SELECT CONCAT(errlog.total,'%') AS error,
            TO_CHAR(errlog.erTime,'FMMonth FMDD , YYYY') 
            AS date FROM errlog GROUP BY errlog.total ,
            errlog.erTime HAVING errlog.total > 1;
            """)
    return c


def print_more_errors():
    print("Errors:")
    for item in more_errors():
        print(str(item[1]) + " -- " + str(item[0]) + " error ")


# printing output of each function


def main():
    print_popular_articles()
    print_popular_authors()
    print_more_errors()


if __name__ == '__main__':
    main()
