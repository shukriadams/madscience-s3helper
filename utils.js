const fs = require('fs'),
    AWS = require('aws-sdk')

module.exports = {

    /**
     * Uploads a local file to s3
     * config : {
     *      accessKeyId: key, 
     *      endpoint, // egs "eu-central-1.linodeobjects.com" for linode
     *      secretAccessKey: secret 
     * }
     */
    putFile : async(config, bucket, localFilepath, writePath) =>{
        return new Promise((resolve, reject) =>{
            AWS.config.update(config)

            const s3 = new AWS.S3()

            fs.readFile(localFilepath, (err, data) =>{

                if(err)
                    return reject(err)

                s3.upload({
                    Bucket: bucket,
                    Key: writePath,
                    Body: data
                }, (err, result) => {
                    if(err)
                        return reject(err)

                    resolve(result)
                })
            })
        })
    },


    /**
     * Writes file content to s3
     * config : {
     *      accessKeyId: key, 
     *      endpoint, // egs "eu-central-1.linodeobjects.com" for linode
     *      secretAccessKey: secret 
     * }
     */
    writeFile : async(config, bucket, writePath, fileContent) =>{
        return new Promise((resolve, reject)=>{

            AWS.config.update(config)

            const s3 = new AWS.S3()

            s3.upload({
                Bucket: bucket,
                Key: writePath,
                Body: Buffer.from(fileContent, 'utf8')
            }, (err, result) => {
                if(err)
                    return reject(err)

                resolve(result)
            })
        })
    },


    /**
     * Gets file as string
     * config : {
     *      accessKeyId: key, 
     *      endpoint, // egs "eu-central-1.linodeobjects.com" for linode
     *      secretAccessKey: secret 
     * }
     */
    getStringFile : async(config, bucket, file)=>{
        return new Promise(async (resolve, reject)=>{
            
            AWS.config.update(config)

            const s3 = new AWS.S3()

            s3.getObject({
                Bucket: bucket,
                Key: file,
            }, (err, res) => {
                if(err)
                    return reject(err)

                let body = res.Body

                // if body is binary, convert to string
                if (typeof(body) === 'object')
                    body = body.toString('utf8')

                resolve(body)
            })

        })
    },

    getBinaryFile : async(config, bucket, file)=>{
        return new Promise(async (resolve, reject)=>{
            
            AWS.config.update(config)

            const s3 = new AWS.S3()

            s3.getObject({
                Bucket: bucket,
                Key: file,
            }, (err, res) => {
                if(err)
                    return reject(err)
                
                console.log('>>>', res.Body)
                resolve(res.Body)
            })

        })
    },

    /**
     * Downloads a file and writes it to local path
     * config : {
     *      accessKeyId: key, 
     *      endpoint, // egs "eu-central-1.linodeobjects.com" for linode
     *      secretAccessKey: secret 
     * }
     */
    downloadFile : async(config, bucket, file, localPath)=>{
        return new Promise(async (resolve, reject)=>{
            
            AWS.config.update(config)

            const s3 = new AWS.S3()

            s3.getObject({
                Bucket: bucket,
                Key: file,
            }, (err, res) => {
                if(err)
                    return reject(err)

                const wstream = fs.createWriteStream(localPath)
                wstream.write(res.Body)
                wstream.end()
                resolve()
            })

        })
    },


    /**
     * Returns true if a file exists on S3
     * config : {
     *      accessKeyId: key, 
     *      endpoint, // egs "eu-central-1.linodeobjects.com" for linode
     *      secretAccessKey: secret 
     * }
     */
    fileExists : async(config, bucket, queryPath) =>{
        return new Promise((resolve, reject)=>{
            
            AWS.config.update(config)

            const s3 = new AWS.S3()

            s3.listObjects({
                Bucket: bucket,
                Prefix : queryPath,
            }, (err, data) => {
                if (err)
                    return reject(err)

                resolve(!!data.Contents.length)

            })
        })
    }   
}
