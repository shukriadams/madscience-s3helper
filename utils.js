const fs = require('fs'),
    AWS = require('aws-sdk')

module.exports = {

    /**
     * Uploads a local file to s3
     */
    putFile : async(key, secret, bucket, localFilepath, writePath) =>{
        return new Promise((resolve, reject) =>{
            AWS.config.update({ 
                accessKeyId: key, 
                secretAccessKey: secret 
            })

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
     */
    writeFile : async(key, secret, bucket, writePath, fileContent) =>{
        return new Promise((resolve, reject)=>{

            AWS.config.update({ 
                accessKeyId: key, 
                secretAccessKey: secret 
            })

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
     */
    getFile : async(key, secret, bucket, file)=>{
        return new Promise(async (resolve, reject)=>{
            
            AWS.config.update({ 
                accessKeyId: key, 
                secretAccessKey: secret 
            })

            const s3 = new AWS.S3()

            s3.getObject({
                Bucket: bucket,
                Key: file,
            }, (err, res) => {
                if(err)
                    return reject(err)

                resolve(res.Body)
            })

        })
    },


    /**
     * Downloads a file and writes it to local path
     */
    downloadFile : async(key, secret, bucket, file, localPath)=>{
        return new Promise(async (resolve, reject)=>{
            
            AWS.config.update({ 
                accessKeyId: key, 
                secretAccessKey: secret 
            })

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
     */
    fileExists : async(key, secret, bucket, queryPath) =>{
        return new Promise((resolve, reject)=>{
            
            AWS.config.update({ 
                accessKeyId: key, 
                secretAccessKey: secret 
            })

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
