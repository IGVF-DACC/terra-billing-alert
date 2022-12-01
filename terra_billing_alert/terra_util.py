import firecloud.api as fapi
import logging


logger = logging.getLogger(__name__)


def get_all_workspaces(namespace):
    '''Retuns a list of NAMEs of all workspaces.
    '''
    result = []
    r = fapi.list_workspaces(fields='workspace.name,workspace.namespace')

    if r.status_code == 200:
        workspace_objs = r.json()
        for workspace_obj in workspace_objs:
            if workspace_obj['workspace']['namespace'] == namespace:
                result.append(workspace_obj['workspace']['name'])
    else:
        logger.error(f'Error retrieving workspaces from namespace {namespace} with error {r.text}')

    logger.info(f'get_all_workspaces: {result}')

    return result


def get_all_submissions(namespace, workspace):
    '''Returns a list of all submission objects.
    '''
    r = fapi.list_submissions(namespace, workspace)

    if r.status_code == 200:
        return r.json()
    else:
        logger.error(f'Error retrieving submission from namespace {namespace} with error {r.text}')

    return {}


def get_all_workflows(namespace, workspace, submission_id):
    '''Retuns a list of workflow objects with matching submission_id.
    '''
    result = []
    r = fapi.get_submission(namespace, workspace, submission_id)

    if r.status_code == 200:
        if 'workflows' in r.json():
            for workflow in r.json()['workflows']:
                result.append(workflow)
    else:
        logger.error(f'Error retrieving submission id {submission_id} with error {r.text}')

    return result


def get_workflow_metadata(namespace, workspace, submission_id, workflow_id):
    r = fapi.get_workflow_metadata(namespace, workspace, submission_id, workflow_id)

    if r.status_code == 200:
        return r.json()
    else:
        logger.error(f'Error retrieving workflow id {workflow_id} with error {r.text}')

    return {}


def get_bucket_usage(namespace, workspace):
    r = fapi.get_bucket_usage(namespace, workspace)

    if r.status_code == 200:
        return r.json()
    else:
        logger.error(f'Error retrieving bucket usage of {workspace} with error {r.text}')

    return {}


def get_all_instances(namespace, workspace):
    # See FireCloud Workbench notebooks API here for details:
    # https://notebooks.firecloud.org/
    # List all Cloud Environment instances
    r = fapi.__get(
        'google/v1/runtimes',
        root_url='https://notebooks.firecloud.org/api/'
    )
    if r.status_code == 200:
        return r.json()
    else:
        logger.error(f'Error retrieving bucket usage of {workspace} with error {r.text}')

    return {}
