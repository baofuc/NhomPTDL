import scrapy
from bookbuy.items import BookbuyItem

class MybookbuySpider(scrapy.Spider):
    name = "mybookbuy"
    allowed_domains = ["bookbuy.vn"]
    start_urls = []

    def start_requests(self):
        urlRelative = 'https://bookbuy.vn/sach/nhung-nguoi-dan-chu-xep-thuyen-tai-ban-2024--p'
        count = 0
        for page in range(52059, 75000):
            count = count + 1
            url = urlRelative + str(page)+'.html'
            print(url)
            print('page - ', count)
            yield scrapy.Request(url, self.parse)
    

    def parse(self, response):
    # Extract and clean book name
        item= BookbuyItem()
        name = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/h1/text()').get().strip()

    # Extract and clean author
        author = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[1]/div[1]/a/h2/text()').get().strip()

    # Extract and clean price
        price = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/p[1]/text()').get().strip()
        price = price.replace(' đ', '').replace(',', '').replace('\t', ' ').strip()

    # Extract and clean market price
        market_price = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/p[2]/text()').get().strip()
        market_price = market_price.replace('Giá thị trường: ', '').replace(' đ', '').replace(',', '').replace('\t', ' ').strip()

    # Extract and clean status
        status = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div[4]/p[2]/text()').get().strip()
        status = status.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip()

    # Extract and clean NXB
        nxb = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[5]/div[1]/div[1]/div/div/div/ul/li[1]/a/text()').get().strip()
        nxb = nxb.replace('\t', ' ').strip().replace('\t', ' ').strip()

    # Extract and clean Publisher
        Publisher = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[5]/div[1]/div[1]/div/div/div/ul/li[3]/a/text()').get().strip()
        Publisher = Publisher.replace('\t', ' ').strip().replace('\t', ' ').strip()

    # Extract and clean date
        date = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[5]/div[1]/div[1]/div/div/div/ul/li[2]/text()').get().strip()
        date = date.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip()
        x = date.split()
        publish_date = x[3]

    # Extract and clean numpage
        numpage = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[5]/div[1]/div[1]/div/div/div/ul/li[5]/span/text()').get().strip()
        numpage = numpage.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip()
        x = numpage.split()
        page = x[0]
    # Extract and clean weight
        weight = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[5]/div[1]/div[1]/div/div/div/ul/li[6]/span/text()').get().strip()
        weight = weight.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip()
        x = weight.split()
        Kg = x[0]

    # Extract and clean content
        content1 = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[4]/div[1]/div[2]/div/div[2]/p[3]/span/span/span/span/text()').get()
        content2 = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[5]/div[1]/div[2]/div/div[2]/p[3]/span/span/span/text()').get()
        content3 = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[5]/div[1]/div[2]/div/div[2]/p[5]/span/span/span/text()').get()
        content4 = response.xpath('//*[@id="bb-body"]/div[1]/div[1]/div[3]/div[4]/div[1]/div[2]/div/div[2]/p[5]/span/span/span/span/text()').get()
        if content1 != None:
            content=content1
        elif content2 !=None:
            content=content2
        elif content4 !=None:
            content=content4
        else :
            content= content3
        content = content.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip()
        item['Book_name']=name
        item['Author']=author
        item['Price']=price
        item['Market_Price']=market_price
        item['Status']=status
        item['Publisher']=nxb
        item['Issuiers']=Publisher
        item['Publish_date']=publish_date
        item['Num_Page']=page
        item['Weight']=Kg
        item['Content']=content

    # Prepare the record to yield
        # record = {
        # "Book_name": name,
        # "Author": author,
        # "Price": price,
        # "Market_Price": market_price,
        # "Status": status,
        # "Publisher": nxb,
        # "Issuiers": Publisher,
        # "Publish_date": publish_date,
        # "Num_Page": page,
        # "Weight": Kg,
        # "Content": content
        # }
    
        yield item

    