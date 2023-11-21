from urllib.parse import urlparse, urljoin
import wget
from bs4 import BeautifulSoup
from requests_html2 import HTMLSession

# initialize the set of links (unique links)
internal_urls = set()
external_urls = set()

total_urls_visited = 0

table_list = set()


def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def get_all_website_links(url):
    """
    Returns all URLs that is found on `url` in which it belongs to the same website
    """
    # all URLs of `url`
    urls = set()
    # domain name of the URL without the protocol
    domain_name = urlparse(url).netloc
    # initialize an HTTP session
    session = HTMLSession()
    # make HTTP request & retrieve response
    response = session.get(url)
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
        href = urljoin(url, href)
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
                print(href)
                external_urls.add(href)
            continue
        print(href)
        urls.add(href)
        internal_urls.add(href)
    return urls


def download_table(url):
    for i in internal_urls:
        if i[-4:] == '.dat':
            dl = wget.download(i)
            table_list.add(dl)
        elif i[-6:] == 'ReadMe':
            wget.download(i)


def table_parser(table_list):
    with open('ReadMe', 'r') as rm:
        for table in table_list:
            count = 0
            stat_table = 0
            for line in rm:
                if table in line:
                    if line.rstrip('\n')[-4:] == '.dat':
                        stat_table = count
                        break
                count += 1
            dic = {}
            for line in rm:
                if count > stat_table + 2:
                    if line[:1] == '-':
                        break
                    if line.split()[0][-1] == '-':
                        dic[line.split()[4]] = line.split()[0][:-1] + '-' + line.split()[1]
                    elif line.split()[0].isdigit():
                        dic[line.split()[3]] = line.split()[0]
                count += 1
            print(dic)
            with open(table, 'r') as fp:
                for key in dic:
                    dic[key]
                    for line in fp:
                        print(line.rstrip('\n'))


if __name__ == "__main__":

    url = "https://cdsarc.cds.unistra.fr/ftp/J/other/AstBu/71.302/"

    get_all_website_links(url)

    print("[+] Total Internal links:", len(internal_urls))
    print("[+] Total External links:", len(external_urls))
    print("[+] Total URLs:", len(external_urls) + len(internal_urls))

    domain_name = urlparse(url).netloc

    # save the internal links to a file
    with open(f"{domain_name}_internal_links.txt", "w") as f:
        for internal_link in internal_urls:
            print(internal_link.strip(), file=f)

    # save the external links to a file
    with open(f"{domain_name}_external_links.txt", "w") as f:
        for external_link in external_urls:
            print(external_link.strip(), file=f)

    download_table(internal_urls)

    table_parser(table_list)
