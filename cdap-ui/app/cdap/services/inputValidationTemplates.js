const NAME = {
    regex: RegExp("^[a-zA-Z][-/\._()a-zA-Z0-9 ]{1,253}[)/a-zA-Z0-9]$"),
    info: "Name needs to between 3 and 255 characters. May contain spaces, underscore, hyphen, period, forward slash, and parentheses. Can start with alphabets only. Can end with alphanumeric characters, forward slash, and closing parentheses.",
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};

/*
- aws_secret_*: https://aws.amazon.com/blogs/security/a-safer-way-to-distribute-aws-credentials-to-ec2/
    - amazon may change the regex for it in the future so this will need to be updated accordingly.
    - relevant stackoverflow thread: https://stackoverflow.com/questions/55623943/validating-aws-access-and-secret-keys
*/
const AWS_ACCESS_KEY_ID = {
    regex: RegExp("(?<![A-Z0-9])[A-Z0-9]{20}(?![A-Z0-9])"),
    info: "Check Standard for AWS Access Key ID",
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};
const AWS_SECRET_ACCESS_KEY = {
    regex: RegExp("(?<![A-Za-z0-9/+=])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])"),
    info: "Check Standard for AWS Secret Access Key",
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};

/*
- project Id: https://cloud.google.com/resource-manager/reference/rest/v1/projects
*/
const GCS_PROJECT_ID = {
    regex: RegExp("^[a-z][-a-z0-9]{4,28}[a-z0-9]$"),
    info: "Check Standard for Google Cloud Project ID",
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};

/*
- service account: https://cloud.google.com/iam/docs/service-accounts
    - Implementation done here is rudimentary and does not cover all edge cases.
*/
const GCS_BUCKET_ID = {
    regex: RegExp("^([a-z0-9][-a-z0-9_\.]{1,220}[a-z0-9])$"),
    info: "Check Standard for Google Cloud Bucket ID",
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};

/*
- hostname: https://tools.ietf.org/html/rfc1123
    - misses excluding some cases including https://tools.ietf.org/html/rfc2181
*/
const HOSTNAME_1123 = {
    regex: RegExp("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$"),
    info: "Check Standard for Hostname in RFC-1123",
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};

const DEFAULT = {
    regex: RegExp(".*"),
    info: "Accepts Everything",
    validate: function(val) {
        return this.regex.test(val) ? true : false;
    }
};

const types = {
    "DEFAULT": DEFAULT,
    "NAME": NAME,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "GCS_PROJECT_ID": GCS_PROJECT_ID,
    "GCS_BUCKET_ID": GCS_BUCKET_ID,
    "HOSTNAME_1123": HOSTNAME_1123
};

// getting some jshint errors here!
// export {
//     DEFAULT,
//     NAME,
// };

export default types;
