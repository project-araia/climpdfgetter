from dataclasses import dataclass


@dataclass
class Source:
    search_base: str


class OSTI(Source):
    search_base = "https://www.osti.gov/search/availability:fulltext/term:{}/publish-date-end:01/01/{}/publish-date-start:01/01/{}/product-type:Journal%20Article/page:{}"  # noqa
    api_base = "https://www.osti.gov/api/v1/records"


class NOAA(Source):
    search_base = "https://repository.library.noaa.gov/cbrowse?pid=noaa%3A9&parentId=noaa%3A9"


class EPA(Source):
    search_base = """https://nepis.epa.gov/Exe/ZyNET.exe?User=ANONYMOUS&Back=ZyActionL&BackDesc=Contents+page&Client=EPA&DefSeekPage=x&Display=hpfr&Docs=&ExtQFieldOp=0&File=&FuzzyDegree=0&ImageQuality=r85g16%2Fr85g16%2Fx150y150g16%2Fi500&Index=2016%20Thru%202020%7C2011%20Thru%202015%7C2006%20Thru%202010%7C2000%20Thru%202005&IndexPresets=entry&IntQFieldOp=0&MaximumDocuments=50&MaximumPages=1&Password=anonymous&QField=&QFieldDay=&QFieldMonth=&QFieldYear=&Query={}%20&SearchBack=ZyActionL&SearchMethod=2&SeekPage=&SortMethod=-&SortMethod=h&Time=&Toc=&TocEntry=&TocRestrict=n&UseQField=&ZyAction=ZyActionS&ZyEntry="""  # noqa: E501


source_mapping = {"EPA": EPA, "NOAA": NOAA, "OSTI": OSTI}
