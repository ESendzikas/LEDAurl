import gzip
from urllib.parse import urlparse, urljoin
import wget
from bs4 import BeautifulSoup
from requests_html2 import HTMLSession
import pandas as pd
from sqlalchemy import create_engine

# initialize the set of links (unique links)
internal_urls = set()
external_urls = set()

total_urls_visited = 0

table_list = set()
table_list_gz = set()


def is_valid(url_valid):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url_valid)
    return bool(parsed.netloc) and bool(parsed.scheme)


def get_all_website_links(url_web):
    """
    Returns all URLs that is found on `url` in which it belongs to the same website.
    """
    # all URLs of `url`
    urls = set()
    # domain name of the URL without the protocol
    domain_name = urlparse(url_web).netloc
    # initialize an HTTP session
    session = HTMLSession()
    # make HTTP request & retrieve response
    response = session.get(url_web)
    # execute Javascript
    try:
        response.render()
    except:
        pass
    soup = BeautifulSoup(response.html.html, "html.parser")
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue
        # join the URL if it's relative (not absolute link)
        href = urljoin(url_web, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        if href in internal_urls:
            # already in the set
            continue
        if domain_name not in href:
            # external link
            if href not in external_urls:
                # print(href)
                external_urls.add(href)
            continue
        # print(href)
        urls.add(href)
        internal_urls.add(href)
    return urls


def download_table(url_internal):
    """
    Download tables from links, creating a list of tables.
    """
    for i in url_internal:
        if i[-4:] == '.dat':
            dl = wget.download(i)
            table_list.add(dl)
        elif i[-7:] == '.dat.gz':
            dl = wget.download(i)
            table_list_gz.add(dl)
            table_list.add(dl[:-3])
        elif i[-6:] == 'ReadMe':
            wget.download(i)


def gz_table(table_gz):
    """
    Table decompressor.
    """
    for url_gz in table_gz:
        f1 = open(url_gz[:-3], "wb")
        f2 = gzip.GzipFile(filename=url_gz)
        f1.write(f2.read())
        f1.close()
        f2.close()


def table_parser(list_of_table):
    """
    Creating summary tables.
    """
    for table in list_of_table:
        with open('ReadMe', 'r') as rm:
            count = 0
            stat_table = 0
            for line_rm in rm:
                if table in line_rm:
                    if line_rm.rstrip('\n')[-4:] == '.dat':
                        stat_table = count
                        break
                count += 1
            dic = {}
            for line in rm:
                if count > stat_table + 2:
                    if line[:1] == '-':
                        break
                    if line.split()[0][-1] == '-':
                        dic[line.split()[4]] = []
                        dic[line.split()[4]].append(line.split()[0][:-1])
                        dic[line.split()[4]].append(line.split()[1])
                        # dic[line.split()[4]].append(line.split()[3])
                        # dic[line.split()[4]].append(line.split()[5:])
                    elif line.split()[0].isdigit():
                        dic[line.split()[3]] = []
                        dic[line.split()[3]].append(line.split()[0])
                        dic[line.split()[3]].append(line.split()[0])
                        # dic[line.split()[3]].append(line.split()[2])
                        # dic[line.split()[3]].append(line.split()[4:])
                count += 1
            dic_data = dic.copy()
            for key_data in dic_data:
                dic_data[key_data] = []
            with open(table, 'r') as fp:
                for line_tb in fp:
                    for key in dic_data:
                        dic_data[key].append(line_tb[int(dic[key][0]) - 1:int(dic[key][1])].strip())
            table_name = table[:-4]
            df = pd.DataFrame(dic_data)
            # df.to_csv(f"{table_name}.csv")
            # df.to_excel(f"{table_name}.xlsx")
            df.to_sql(table_name, con=engine, if_exists='replace')


if __name__ == "__main__":
    url = "https://cdsarc.cds.unistra.fr/ftp/J/other/MNRAS/527/10668/"

    get_all_website_links(url)

    download_table(internal_urls)

    gz_table(table_list_gz)

    # Connect PGSQL
    engine = create_engine('postgresql+psycopg2://testuser:90210556@localhost:5432/hl')

    table_parser(table_list)
