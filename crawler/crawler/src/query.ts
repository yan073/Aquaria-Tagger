import mongodb from "mongodb";
import axios from "axios";
import moment from "moment";

const articlesCollectionName = 'trial';
const crawling_delay = 5000 // milli-seconds

const articleMetadataList = ['>First Submitted Date', 
                            '>First Posted Date',
                            '>Last Update Posted Date',
                            '>Brief Title',
                            '>Official Title',
                            '>Brief Summary',
                            '>Detailed Description',
                            '>Study Design',
                            '>Publications *',
                            '>Listed Location Countries' ];

interface Untagged{
    firstSubmittedDate: string;
    firstPostedDate: string;
    lastUpdatePostedDate: string;
    briefTitle: string;
    officialTitle: string;
    briefSummary: string;
    detailedDescription: string;
    studyDesign: string;
    publications: string;
    listedLocationCountries: string;
}

interface Content {
    url: string;
    ctid: string;
    untagged: Untagged;
    capturedDate: string;
}; 

const getClinicalTrialsId = (url: string) => {
    let index = url.indexOf('show/record/');
    if (index > 0) {
        return url.substring(index + 'show/record/'.length );
    }
    return '';
}

const fetchRecord = async(r: any, db: mongodb.Db) => {
    let ctid = getClinicalTrialsId(r);
    console.log(`-- fetching url: ${r}`);
    let response = await axios.get(r);
    if (response.status == 200) {
        console.log(`\t parsing ${r}...`);
        let content = parseArticleInfo(response.data);    
        content.ctid = ctid;
        content.url = r;
        console.log(`\t writing ${r}...`);
        await db.collection(articlesCollectionName).insertOne( content );
    } else {
        console.log(`  ERROR, status code is ${response.status}`);
    }
    
}

const delay = ms => new Promise(res => setTimeout(res, ms));

export const queryArticles = async (urls: string[], db: mongodb.Db) => {
    const articleCollection = db.collection(articlesCollectionName);
    for (let r of urls) {
        try{
            let found = await articleCollection.findOne( {url: r} );
            if (! found) {
                await delay(crawling_delay); 
                await fetchRecord(r, db);
            }
        }
        catch(e) {
            console.error(e);
        }
    }
}

function findCellContentFromRowHeader(text: string, header: string) : string {
    let content = getRawCellContentFromRowHeader(text, header);
    return cleanUp(content);
}   

function cleanup_removeOnclicks(content: string): string {
    let oldlength = 999999999;
    while (content.length < oldlength) {
        oldlength = content.length;
        content = cleanup_removeOnclick(content);
    }
    return content;
}

function cleanup_removeOnclick(content: string): string {
    const ulstart = " onclick=\"";
    let start = content.indexOf(ulstart);
    if (start >= 0) {
        let after = content.substring(start + ulstart.length);
        let end = after.indexOf("\">");
        if( end > 0) {
            let newstr = '';
            if (start > 0) {
                newstr = content.substring(0, start);
            }
            content = newstr.concat(after.substring(end +1)); // keep ">"
        }
    }
    return content;
}

function cleanup_removeElement(content: string, header: string): string {
    let start = content.indexOf(header);
    if (start >= 0) {
        let after = content.substring(start + header.length);
        let end = after.indexOf("\">");
        if( end > 0) {
            let newstr = '';
            if (start > 0) {
                newstr = content.substring(0, start);
            }
            content = newstr.concat(after.substring(end +2));
        }
    }
    return content;
}

function cleanup_removeUl(content: string): string {
    return cleanup_removeElement(content, "<ul style=\"");
}

function cleanup_removeli(content: string): string {
    let oldlength = 999999999;
    while (content.length < oldlength) {
        oldlength = content.length;
        content = cleanup_removeElement(content, "<li style=\"");
    }
    return content;
}

function cleanUp(content: string): string {
    return cleanup_removeOnclicks(cleanup_removeli(cleanup_removeUl(content)))
                .trim();
}

function getRawCellContentFromRowHeader(text: string, header: string) : string {
    let index = text.indexOf(header);
    if (index >0) {
        let trtext = text.substring(index);
        let tdIndex = trtext.indexOf('<td ');
        let tdEndIndex = trtext.indexOf('</td>');
        if (tdEndIndex > tdIndex) {
            let tdtext = trtext.substring(tdIndex, tdEndIndex);
            return tdtext.substring(tdtext.indexOf('>')+1);
        }
    }
    return '';
}

function parseArticleInfo(xmlbody: string) : Content {
    let content : Content ={
        capturedDate : moment().format('MMMM DD YYYY'),
        url: '',
        ctid: '',
        untagged: {
            firstSubmittedDate: findCellContentFromRowHeader(xmlbody, articleMetadataList[0]),
            firstPostedDate:  findCellContentFromRowHeader(xmlbody, articleMetadataList[1]),
            lastUpdatePostedDate: findCellContentFromRowHeader(xmlbody, articleMetadataList[2]),
            briefTitle: findCellContentFromRowHeader(xmlbody, articleMetadataList[3]),
            officialTitle: findCellContentFromRowHeader(xmlbody, articleMetadataList[4]),
            briefSummary: findCellContentFromRowHeader(xmlbody, articleMetadataList[5]),
            detailedDescription: findCellContentFromRowHeader(xmlbody, articleMetadataList[6]),
            studyDesign: findCellContentFromRowHeader(xmlbody, articleMetadataList[7]),
            publications: findCellContentFromRowHeader(xmlbody, articleMetadataList[8]),
            listedLocationCountries: findCellContentFromRowHeader(xmlbody, articleMetadataList[9]),
        },
    };
    return content;
}
