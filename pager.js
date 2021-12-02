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
     * Gets page of items from bucket. Item structure is
     *  {
     *      Key: string, path+filename within bucket,
     *      LastModified: string datetime,
     *      ETag: string UUID,
     *      Size: int bytes ,
     *      StorageClass: string egs 'STANDARD',
     *      Owner: {
     *          ID: string uuid
     *      }
     *   }
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
                    if (key ===  `${this.subfolder}/`) 
                        continue
    
                    items.push(data.Contents[i])
                }
                
                if (data.Contents.length)
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
            const page = await this.getNextPage()
            items = items.concat(page)
        }
        
        return items
    }   
    
}   
