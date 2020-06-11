const AWS = require('aws-sdk')

module.exports = class {
    
    constructor(key, secret, bucket, subfolder = '', startKey = ''){
        AWS.config.update({ accessKeyId: key, secretAccessKey: secret })
        this.s3 = new AWS.S3()
        this.startKey = startKey
        this.canFetchMore = false
        this.bucket = bucket
        this.subfolder = subfolder
        this.pageSize = 1000
        this.canFetchMore = true
    }

    /**
     * Gets page of item from bucket
     */
    async getNextPage (){
        
        this.canFetchMore = false

        return new Promise((resolve, reject)=>{
            this.s3.listObjects({
                Bucket: this.bucket,
                Prefix : this.subfolder,
                Marker: this.startKey,
                MaxKeys: this.pageSize,
            }, function(err, data) {

                if (err)
                    return reject(err)
    
                const items = []
    
                for (let i = 0; i < data.Contents.length; i++) {
                    const key = data.Contents[i].Key   
    
                    // ignore root search folder
                    if (key ===  `${subfolder}/`) 
                        continue
    
                    items.push(data.Contents[i])
                }
                
                this.startKey = data.Contents[data.Contents.length - 1].Key
                this.canFetchMore = data.IsTruncated
    
                resolve(items)
            })
    
        })

    }


    /**
     * Gets all items in bucket
     */
    async getAll (){
        let items = []

        while (this.canFetchMore){
            const page = await this._page()
            items = items.concat(page)
        }
        
        return items
    }   
    
}