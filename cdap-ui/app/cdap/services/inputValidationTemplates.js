import DOMPurify from 'dompurify';

const NAME = {
    regex: RegExp("^[-@,:\.()a-zA-Z0-9 ]+$"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "Input should be alphanumeric and may contain spaces, underscore, hyphen, period, comma, forward slash, @, and parentheses.",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};

const FILE_PATH = {
    regex: RegExp("^(.+)\/([^/]+)$"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "File path cannot end with forward slash",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};

/*
- aws_secret_*: https://aws.amazon.com/blogs/security/a-safer-way-to-distribute-aws-credentials-to-ec2/
    - amazon may change the regex for it in the future so this will need to be updated accordingly.
    - relevant stackoverflow thread: https://stackoverflow.com/questions/55623943/validating-aws-access-and-secret-keys
*/
const AWS_ACCESS_KEY_ID = {
    regex: RegExp("(?<![A-Z0-9])[A-Z0-9]{20}(?![A-Z0-9])"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "Check Standard for AWS Access Key ID",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};
const AWS_SECRET_ACCESS_KEY = {
    regex: RegExp("(?<![A-Za-z0-9/+=])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "Check Standard for AWS Secret Access Key",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};

/*
- project Id: https://cloud.google.com/resource-manager/reference/rest/v1/projects
*/
const GCS_PROJECT_ID = {
    regex: RegExp("^[a-z][-a-z0-9]{4,28}[a-z0-9]$"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "Input must be 6 to 30 lowercase letters, digits, or hyphens. It must start with a letter. Trailing hyphens are prohibited.",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};

/*
- service account: https://cloud.google.com/iam/docs/service-accounts
    - Implementation done here is rudimentary and does not cover all edge cases.
*/
const GCS_BUCKET_ID = {
    regex: RegExp("^([a-z0-9][-a-z0-9_\.]{1,220}[a-z0-9])$"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "Check Standard for Google Cloud Bucket ID: https://cloud.google.com/iam/docs/service-accounts",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};

/*
- hostname: https://tools.ietf.org/html/rfc1123
    - misses excluding some cases including https://tools.ietf.org/html/rfc2181
*/
const HOSTNAME_1123 = {
    regex: RegExp("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "Check Standard for Hostname in RFC-1123",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};

const DEFAULT = {
    regex: RegExp(".*"),
    dom_sanitizer: DOMPurify.sanitize,
    allowed: {
        ALLOWED_TAGS: [],
    },
    info: [
        "Accepts Everything",
        "cannot contain any xml tags."
    ],
    validate: function(val, useDOM = true) {
        if (useDOM) {
            const clean = this.dom_sanitizer(val, this.allowed);
            return clean === val ? true : false;
        }
        return this.regex.test(val) ? true : false;
    },
    getInfo: function(val, useDOM = true) {
        return (useDOM ? this.info[1] : this.info[0]);
    }
};

const types = {
    "DEFAULT": DEFAULT,
    "NAME": NAME,
    "FILE_PATH": FILE_PATH,
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
