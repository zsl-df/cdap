import DOMPurify from 'dompurify';

const TEMPLATES = {
    'simple': {
        ALLOWED_TAGS: [],
    },
};

/* 
REGEX SOURCES
-------------

AWS
===

- aws_secret_*: https://aws.amazon.com/blogs/security/a-safer-way-to-distribute-aws-credentials-to-ec2/
    - amazon may change the regex for it in the future so this will need to be updated accordingly.
    - relevant stackoverflow thread: https://stackoverflow.com/questions/55623943/validating-aws-access-and-secret-keys

Google Cloud
============

- project Id: https://cloud.google.com/resource-manager/reference/rest/v1/projects
- service accound: https://cloud.google.com/iam/docs/service-accounts
    - Implementation done here is rudimentary and does not cover all edge cases.
*/
const REGEX = {
    'aws_access_key_id': new RegExp("(?<![A-Z0-9])[A-Z0-9]{20}(?![A-Z0-9])"),
    'aws_secret_access_key': new RegExp("(?<![A-Za-z0-9/+=])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])"),
    'gcs_project_id': new RegExp("^[a-z][-a-z0-9]{4,28}[a-z0-9]$"),
    'gcs_bucket': new RegExp("^([a-z0-9][-a-z0-9_\.]{1,220}[a-z0-9])$")
};

const dom_sanitizer = DOMPurify.sanitize;

const inputSanitizer = ({dirty = '', inputName = '', config = 'simple'} = {}) => {

    if (Object.keys(REGEX).includes(config)) {
        const clean = (REGEX[config].test(dirty) ? dirty : '');
        return {
            'clean': clean,
            'error': clean ? null : "Invalid Input" + (inputName ? ': ' + inputName : inputName)
        };
    }

    if (config === 'uri') {
        return {
            'clean': encodeURIComponent(dirty),
            'error': null
        };
    }

    const clean = dom_sanitizer(dirty, TEMPLATES[config]);
    // TODO: need a better error code!
    const error = 'Invalid Input' + (inputName ? ': '  + inputName : inputName);
    return {
        'clean': clean,
        'error': clean !== dirty ? error : null,
    };
};

export default inputSanitizer;
