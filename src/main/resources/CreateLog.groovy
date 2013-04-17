def targetFile = new File("/tmp/storm/log.log")
rand = new Random()

while (true) {
    printLine(targetFile);
    Thread.sleep(250)
}


class Constants {
    static final countries = ['US', 'AU', 'FR', 'FL', 'RU']
    static final urls = ["/", "/prices.do", "/dashboard.do"]
    static final responseCodes = [200, 404, 503]
}


def getCountry() {
    return Constants.countries[ rand.nextInt(Constants.countries.size()) ]
}


def getUrl() {
    return Constants.urls[ rand.nextInt(Constants.urls.size()) ]
}


def getResponseCode() {
    return Constants.responseCodes[ rand.nextInt(Constants.responseCodes.size()) ]
}

def getResponseTime() {
    return rand.nextInt(50)
}

def printLine(def f) {
    def line = String.format("%d,%s,%s,%d,%d\n", System.currentTimeMillis(), getCountry(), getUrl(), getResponseCode(), getResponseTime())
    f.append(line)
}