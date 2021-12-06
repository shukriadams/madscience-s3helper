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
            try {
                AWS.config.update(config)

                const s3 = new AWS.S3(),
                    Stream = require('stream'),
                    readStream = fs.createReadStream(localFilepath),
                    pass = new Stream.PassThrough()
    
                readStream
                  .pipe(pass)
    
                s3.upload({
                    Bucket: bucket,
                    Key: writePath,
                    Body: pass
                }, (err, result) => {
                    if(err)
                        return reject(err)
    
                    resolve(result)
                })
            } catch (ex) {
                reject(ex)
            }
        })
    },


    /**
     * Deletes a file on S3.
     */
    deleteFile : async(config, bucket, filePath) =>{
        return new Promise((resolve, reject) =>{
            try {
                AWS.config.update(config)

                const s3 = new AWS.S3()

                s3.deleteObject({
                    Bucket: bucket,
                    Key: filePath
                }, err => {
                    if (err) 
                        return reject(err)
    
                    resolve()
                })
            } catch (ex){
                reject(ex)
            }
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
            try {
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
            } catch (ex){
                reject(ex)
            }
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
            try {
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
            } catch (ex){
                reject(ex)
            }

        })
    },

    getBinaryFile : async(config, bucket, file)=>{
        return new Promise(async (resolve, reject)=>{
            try {
                AWS.config.update(config)

                const s3 = new AWS.S3()
    
                s3.getObject({
                    Bucket: bucket,
                    Key: file,
                }, (err, res) => {
                    if(err)
                        return reject(err)
                    
                    resolve(res.Body)
                })
            } catch (ex){
                reject(ex)
            }

        })
    },



    /**
     * avoid using this, aws sdk will throw an unhandled 'nosuchkey' or 'SignatureDoesNotMatch' exception if the file you're tring to stream can't be found.
     * 
     */
    streamFile : async(config, bucket, file, stream)=>{
        return new Promise(async (resolve, reject)=>{
            try {
                AWS.config.update(config)

                const s3 = new AWS.S3()
                
                const s3Stream = s3.getObject({
                    Bucket: bucket,
                    Key: file,
                }).createReadStream()
    
                s3Stream.pipe(stream)
                    .on('error', err => {
                        reject(err)
                    }).on('close', ()=>{
                        resolve()
                    })
    
            } catch (ex){
                reject(ex)
            }
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
            try {
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
            } catch (ex){
                reject(ex)
            }

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
        AWS.config.update(config)
        const s3 = new AWS.S3()

        return await s3
            .headObject({
                Bucket: bucket,
                Key: queryPath,
            })
            .promise()
            .then(
                function(){ return true },
                function(err){
                    if (err.code === 'NotFound') 
                        return false

                    throw err
                }
            )
    }   
}
