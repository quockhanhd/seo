import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import mysql.connector
from urllib.parse import urlparse
import tldextract


def insertNewDomains(url, mycursor, mydb):
    ext = tldextract.extract(url)
    parsed_uri = urlparse(url)
    uri = parsed_uri
    domain = uri.netloc
    domain = domain.replace(ext.domain+'.', '')
    sql = "INSERT IGNORE INTO domains (domain, protocol, subdomain) VALUES (%s, %s, %s)"
    val = (ext.domain + '.' + ext.suffix, uri.scheme, ext.subdomain)
    mycursor.execute(sql, val)
    mydb.commit()


def insertSCRAP(mycursor, mydb, domain, url, tag, atribute, value, text, control):
    try:
        sql = u"INSERT IGNORE INTO scrap (domain, url, tag, atribute, value, text, control) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (domain, url, tag, atribute, value, text, control)
        mycursor.execute(sql, val)
        mydb.commit()
        print('IN: ' + url + " ||" + tag + ": " + value + " - ||" + text)
    except requests.exceptions.RequestException as e:
        print(e)
        print("error insertSCRAP")


def executeCrawleo(domainExacto, url, subdomain, protocol, mycursor, mydb):
    if subdomain == "":
        subdomain = ""
    else:
        subdomain = subdomain + "."
    domain = protocol + '://' + subdomain + domainExacto + '/'
    urlCompleta = urljoin(domain, url)
    headers = {
        'User-Agent': 'MollaBot 0.1',
        'From': 'quockhanhitdlu@gmail.com'
    }
    print("execute: " + urlCompleta)
    try:
        req = requests.get(urlCompleta, headers=headers)
        soup = BeautifulSoup(req.text, "lxml")

        for tag in soup.find_all('h1'):
            insertSCRAP(mycursor, mydb, domainExacto, urlCompleta,
                        'h1', '', '', tag.text, domain + url + 'h1' + tag.text)

        for tag in soup.find_all('title'):
            insertSCRAP(mycursor, mydb, domainExacto, urlCompleta, 'title',
                        '', '', tag.text, domain + url + 'title' + tag.text)

        for a in soup.find_all('a', href=True):
            urlCompleta = urljoin(domain, url)
            insertSCRAP(mycursor, mydb, domainExacto, urlCompleta, 'a', 'href',
                        a['href'], a.text, domain + url + 'a' + 'href' + a['href'] + a.text)

            if '://' in a['href']:
                insertNewDomains(a['href'], mycursor, mydb)

    except requests.exceptions.RequestException as e:
        print(e)
        print("error executeCrawleo")


def Crawler():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="toolseo",
        charset='utf8',
        use_unicode=True
    )
    mycursor = mydb.cursor()
    print('db connected')
    mycursor.execute(
        "SELECT domain, status, subdomain, protocol FROM domainS WHERE status != 1;")
    myresult = mycursor.fetchall()
    for x in myresult:
        domain = x[0]
        status = x[1]
        subdomain = x[2]
        protocol = x[3]
        if status == -1:
            executeCrawleo(domain, '', subdomain, protocol, mycursor, mydb)
            sql = "UPDATE domains SET status = '0' WHERE domain = '"+domain+"'"
            mycursor.execute(sql)
            mydb.commit()
        else:
            sql = "SELECT DISTINCT value,domain FROM SCRAP WHERE value NOT IN (select url FROM SCRAP) AND (value LIKE '%" + \
                domain+"%') AND value NOT LIKE '%#%' AND value LIKE '%/%';"
            mycursor.execute(sql)
            myresult = mycursor.fetchall()
            totalUrls = len(myresult)
            print("totalUrls: " + str(totalUrls))
            if totalUrls > 0:
                for x in myresult:
                    executeCrawleo(x[1], x[0], subdomain,
                                   protocol, mycursor, mydb)
            else:
                sql = "UPDATE domains SET status = '1' WHERE domain = '"+domain+"'"
                mycursor.execute(sql)
                mydb.commit()
    mycursor.close()


while True:
    Crawler()
