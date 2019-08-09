import datetime
import json
import logging
import re
import time
import uuid
from subprocess import TimeoutExpired, CalledProcessError
from typing import List, Union, Tuple, Dict, Any, Optional, Generator

import kubernetes
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.stream.ws_client import ERROR_CHANNEL, STDOUT_CHANNEL, STDERR_CHANNEL

from benji.helpers import settings
from benji.helpers.constants import LABEL_INSTANCE, LABEL_K8S_PVC_NAMESPACE, LABEL_K8S_PVC_NAME, LABEL_K8S_PV_NAME, \
    LABEL_K8S_STORAGE_CLASS_NAME, LABEL_K8S_PV_TYPE, LABEL_RBD_CLUSTER_FSID, \
    LABEL_RBD_IMAGE_SPEC, PV_TYPE_RBD, VERSION_DATE, VERSION_VOLUME, VERSION_SNAPSHOT, VERSION_SIZE, VERSION_STORAGE, \
    VERSION_BYTES_READ, VERSION_BYTES_WRITTEN, VERSION_BYTES_DEDUPLICATED, VERSION_BYTES_SPARSE, VERSION_DURATION, \
    VERSION_UID, RESOURCE_SPEC_VOLUME_INFO_RBD_CLUSTER_FSID, \
    RESOURCE_SPEC_VOLUME_INFO_RBD_IMAGE_SPEC, RESOURCE_SPEC_VOLUME_INFO_PERSISTENT_VOLUME_NAME, \
    RESOURCE_SPEC_VOLUME_INFO_STORAGE_CLASS_NAME, \
    RESOURCE_SPEC_VOLUME_INFO_PERSISTENT_VOLUME_CLAIM_NAME, RESOURCE_SPEC_DATE, RESOURCE_SPEC_VOLUME, \
    RESOURCE_SPEC_SNAPSHOT, RESOURCE_SPEC_SIZE, RESOURCE_SPEC_STORAGE, RESOURCE_SPEC_BYTES_READ, \
    RESOURCE_SPEC_BYTES_WRITTEN, RESOURCE_SPEC_BYTES_DEDUPLICATED, RESOURCE_SPEC_BYTES_SPARSE, RESOURCE_SPEC_DURATION, \
    RESOURCE_SPEC_VOLUME_INFO, RESOURCE_STATUS_PROTECTED, RESOURCE_STATUS_STATUS, VERSION_PROTECTED, VERSION_STATUS, \
    VERSION_LABELS
from benji.helpers.settings import running_pod_name, benji_instance

SERVICE_NAMESPACE_FILENAME = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

BENJI_VERSIONS_API_VERSION = 'v1alpha1'
BENJI_VERSIONS_API_GROUP = 'benji-backup.me'
BENJI_VERSIONS_API_PLURAL = 'benjiversions'

EVENT_REPORTING_COMPONENT = 'benji'

logger = logging.getLogger()


def load_config() -> None:
    try:
        kubernetes.config.load_incluster_config()
        logger.debug('Configured in cluster with service account.')
    except Exception:
        try:
            kubernetes.config.load_kube_config()
            logger.debug('Configured via kubeconfig file.')
        except Exception:
            raise RuntimeError('No Kubernetes configuration found.')


def service_account_namespace() -> str:
    with open(SERVICE_NAMESPACE_FILENAME, 'r') as f:
        namespace = f.read()
        if namespace == '':
            raise RuntimeError(f'{SERVICE_NAMESPACE_FILENAME} is empty.')
    return namespace


# This was implemented with version 10.0.0 of the Python kubernetes client in mind. But there are several open issues
# and PRs regarding encoding and timeout with might affect us in the future:
#
#  https://github.com/kubernetes-client/python-base/issues/106
#  https://github.com/kubernetes-client/python-base/pull/143
#  https://github.com/kubernetes-client/python-base/pull/78
#
# kubectl uses a POST request to establish the pod connection. We mimic this here by using
# connect_post_namespaced_pod_exec. The examples from the kubernetes client use connect_get_namespaced_pod_exec instead.
# There shouldn't be any differences in functionality but the settings in the RBAC role are different (create vs. get)
# which is why we follow the kubectl implementation here.
def pod_exec(args: List[str],
             *,
             name: str,
             namespace: str,
             container: str = None,
             timeout: float = float("inf")) -> Tuple[str, str]:
    core_v1_api = kubernetes.client.CoreV1Api()
    logger.debug('Running command in pod {}/{}: {}.'.format(namespace, name, ' '.join(args)))
    ws_client = stream(core_v1_api.connect_post_namespaced_pod_exec,
                       name,
                       namespace,
                       command=args,
                       container=container,
                       stderr=True,
                       stdin=False,
                       stdout=True,
                       tty=False,
                       _preload_content=False)

    start = time.time()
    while ws_client.is_open() and time.time() - start < timeout:
        ws_client.update(timeout=(timeout - time.time() + start))

    stdout_channel = ws_client.read_channel(STDOUT_CHANNEL, timeout=0)
    stderr_channel = ws_client.read_channel(STDERR_CHANNEL, timeout=0)
    error_channel = ws_client.read_channel(ERROR_CHANNEL, timeout=0)
    ws_client.close()
    if error_channel == '':
        raise TimeoutExpired(cmd=args, timeout=timeout, output=stdout_channel, stderr=stderr_channel)
    else:
        error_channel_object = json.loads(error_channel)

        # Failure example:
        # {
        #   "metadata": {},
        #   "status": "Failure",
        #   "message": "command terminated with non-zero exit code: Error executing in Docker Container: 126",
        #   "reason": "NonZeroExitCode",
        #   "details": {
        #     "causes": [
        #       {
        #         "reason": "ExitCode",
        #         "message": "126"
        #       }
        #     ]
        #   }
        # }
        #
        # Non-zero exit codes from the command ran are also returned this way.
        #
        # Success example:
        # {"metadata":{},"status":"Success"}
        #
        # See: https://github.com/kubernetes/kubernetes/blob/87b744715ec6952c45d04253dc7b63fc3cfe1ddc/staging/src/k8s.io/client-go/tools/remotecommand/v4.go#L82
        #      https://github.com/kubernetes-client/python-base/blob/master/stream/ws_client.py

        assert isinstance(error_channel_object, dict)
        assert 'status' in error_channel_object
        if error_channel_object['status'] == 'Success':
            pass
        elif error_channel_object['status'] == 'Failure' and 'reason' in error_channel_object and error_channel_object['reason'] == 'NonZeroExitCode':
            assert 'details' in error_channel_object
            assert 'causes' in error_channel_object['details']
            assert isinstance(error_channel_object['details']['causes'], list)
            for cause in error_channel_object['details']['causes']:
                assert 'reason' in cause
                if cause['reason'] != 'ExitCode':
                    continue
                assert 'message' in cause
                raise CalledProcessError(returncode=int(cause["message"]),
                                         cmd=args,
                                         output=stdout_channel,
                                         stderr=stderr_channel)
        else:
            raise RuntimeError(f'Unknown stream status: {error_channel_object["status"]}/{error_channel_object.get("reason", "mot-set")}.')

    return stdout_channel, stderr_channel


def create_pvc_event(*, type: str, reason: str, message: str, pvc_namespace: str, pvc_name: str,
                     pvc_uid: str) -> kubernetes.client.models.v1_event.V1Event:
    event_name = '{}-{}'.format(benji_instance, str(uuid.uuid4()))
    # Kubernetes requires a time including microseconds
    event_time = datetime.datetime.utcnow().isoformat(timespec='microseconds') + 'Z'

    # Setting uid is required so that kubectl describe finds the event.
    # And setting firstTimestamp is required so that kubectl shows a proper age for it.
    # See: https://github.com/kubernetes/kubernetes/blob/
    event = {
        'apiVersion': 'v1',
        'kind': 'Event',
        'metadata': {
            'name': event_name,
            'namespace': pvc_namespace,
            'labels': {
                LABEL_INSTANCE: benji_instance
            }
        },
        'involvedObject': {
            'apiVersion': 'v1',
            'kind': 'PersistentVolumeClaim',
            'name': pvc_name,
            'namespace': pvc_namespace,
            'uid': pvc_uid
        },
        'eventTime': event_time,
        'firstTimestamp': event_time,
        'lastTimestamp': event_time,
        'type': type,
        'reason': reason,
        # Message can be at most 1024 characters long
        'message': message[:1024],
        'action': 'None',
        'reportingComponent': EVENT_REPORTING_COMPONENT,
        'reportingInstance': running_pod_name,
        'source': {
            'component': 'benji'
        }
    }

    core_v1_api = kubernetes.client.CoreV1Api()
    return core_v1_api.create_namespaced_event(namespace=pvc_namespace, body=event)


def create_pvc(*, pvc_name: str, pvc_namespace: str, pvc_size: str,
               storage_class_name: str) -> kubernetes.client.models.v1_persistent_volume_claim.V1PersistentVolumeClaim:
    pvc = {
        'kind': 'PersistentVolumeClaim',
        'apiVersion': 'v1',
        'metadata': {
            'namespace': pvc_namespace,
            'name': pvc_name,
        },
        'spec': {
            'storageClassName': storage_class_name,
            'accessModes': ['ReadWriteOnce'],
            'resources': {
                'requests': {
                    'storage': pvc_size
                }
            }
        }
    }

    core_v1_api = kubernetes.client.CoreV1Api()
    return core_v1_api.create_namespaced_persistent_volume_claim(namespace=pvc_namespace, body=pvc)


def update_version_resource(*, version: Dict[str, Any]) -> Dict[str, Any]:
    labels = version[VERSION_LABELS]

    required_label_names = [
        LABEL_INSTANCE, LABEL_K8S_PVC_NAME, LABEL_K8S_PVC_NAMESPACE, LABEL_K8S_PV_NAME, LABEL_K8S_PV_TYPE,
        LABEL_K8S_STORAGE_CLASS_NAME
    ]

    if LABEL_K8S_PV_TYPE in labels:
        pv_type = labels[LABEL_K8S_PV_TYPE]
    else:
        raise KeyError(f'Version {version[VERSION_UID]} is missing label {LABEL_K8S_PV_TYPE}, skipping update.')

    if pv_type == PV_TYPE_RBD:
        required_label_names.extend((LABEL_RBD_CLUSTER_FSID, LABEL_RBD_IMAGE_SPEC))

    for label_name in required_label_names:
        if label_name not in labels:
            raise KeyError(f'Version {version["uid"]} is missing label {label_name}, skipping update.')

    namespace = labels[LABEL_K8S_PVC_NAMESPACE]
    logger.debug(f'Updating version resource {namespace}/{version["uid"]}.')

    pv_fields: Dict[str, Any] = {}
    if pv_type == PV_TYPE_RBD:
        pv_fields = {
            RESOURCE_SPEC_VOLUME_INFO_RBD_CLUSTER_FSID: labels[LABEL_RBD_CLUSTER_FSID],
            RESOURCE_SPEC_VOLUME_INFO_RBD_IMAGE_SPEC: labels[LABEL_RBD_IMAGE_SPEC],
        }

    body: Dict[str, Any] = {
        'apiVersion': 'benji-backup.me/v1alpha1',
        'kind': 'BenjiVersion',
        'metadata': {
            'name': version['uid'],
            'namespace': namespace,
            'annotations': {},
            'labels': {
                LABEL_INSTANCE: labels[LABEL_INSTANCE],
            },
        },
        'spec': {
            RESOURCE_SPEC_DATE: version[VERSION_DATE],
            RESOURCE_SPEC_VOLUME: version[VERSION_VOLUME],
            RESOURCE_SPEC_SNAPSHOT: version[VERSION_SNAPSHOT],
            RESOURCE_SPEC_SIZE: str(version[VERSION_SIZE]),
            RESOURCE_SPEC_STORAGE: version[VERSION_STORAGE],
            RESOURCE_SPEC_BYTES_READ: str(version[VERSION_BYTES_READ]),
            RESOURCE_SPEC_BYTES_WRITTEN: str(version[VERSION_BYTES_WRITTEN]),
            RESOURCE_SPEC_BYTES_DEDUPLICATED: str(version[VERSION_BYTES_DEDUPLICATED]),
            RESOURCE_SPEC_BYTES_SPARSE: str(version[VERSION_BYTES_SPARSE]),
            RESOURCE_SPEC_DURATION: version[VERSION_DURATION],
            RESOURCE_SPEC_VOLUME_INFO: {
                RESOURCE_SPEC_VOLUME_INFO_PERSISTENT_VOLUME_CLAIM_NAME: labels[LABEL_K8S_PVC_NAME],
                RESOURCE_SPEC_VOLUME_INFO_PERSISTENT_VOLUME_NAME: labels[LABEL_K8S_PV_NAME],
                RESOURCE_SPEC_VOLUME_INFO_STORAGE_CLASS_NAME: labels[LABEL_K8S_STORAGE_CLASS_NAME],
            },
        },
        'status': {
            RESOURCE_STATUS_PROTECTED: version[VERSION_PROTECTED],
            RESOURCE_STATUS_STATUS: version[VERSION_STATUS].capitalize(),
        }
    }

    if pv_fields:
        body['spec'][RESOURCE_SPEC_VOLUME_INFO][pv_type] = pv_fields

    version_resource: Optional[Dict[str, Any]] = None
    custom_objects_api = kubernetes.client.CustomObjectsApi()
    try:
        version_resource = get_version_resource(version['uid'], namespace)

        body['metadata']['resourceVersion'] = version_resource['metadata']['resourceVersion']

        # Keep other labels and annotations but overwrite our own
        version_resource['metadata']['labels'] = version_resource['metadata'].get('labels', {})
        version_resource['metadata']['labels'].update(body['metadata']['labels'])
        body['metadata']['labels'] = version_resource['metadata']['labels']

        version_resource['metadata']['annotations'] = version_resource['metadata'].get('annotations', {})
        version_resource['metadata']['annotations'].update(body['metadata']['annotations'])
        body['metadata']['annotations'] = version_resource['metadata']['annotations']

        # Keep other status field but overwrite protected and status
        version_resource['status'] = version_resource.get('status', {})
        version_resource['status'].update(body['status'])
        body['status'] = version_resource['status']

        version_resource = custom_objects_api.replace_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                                               version=BENJI_VERSIONS_API_VERSION,
                                                                               plural=BENJI_VERSIONS_API_PLURAL,
                                                                               name=version['uid'],
                                                                               namespace=namespace,
                                                                               body=body)
    except ApiException as exception:
        if exception.status == 404:
            version_resource = custom_objects_api.create_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                                                  version=BENJI_VERSIONS_API_VERSION,
                                                                                  plural=BENJI_VERSIONS_API_PLURAL,
                                                                                  namespace=namespace,
                                                                                  body=body)
        else:
            raise exception

    assert isinstance(version_resource, dict)
    return version_resource


def delete_version_resource(name: str, namespace: str) -> None:
    custom_objects_api = kubernetes.client.CustomObjectsApi()

    try:
        logger.debug(f'Deleting version resource {namespace}/{name}.')
        custom_objects_api.delete_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                           version=BENJI_VERSIONS_API_VERSION,
                                                           plural=BENJI_VERSIONS_API_PLURAL,
                                                           name=name,
                                                           namespace=namespace,
                                                           body=kubernetes.client.V1DeleteOptions())
    except ApiException as exception:
        if exception.status == 404:
            logger.warning(f'Tried to delete non-existing version resource {name} in namespace {namespace}.')
        else:
            raise exception


def list_namespaces(
        label_selector: str = '') -> Generator[kubernetes.client.models.v1_namespace.V1Namespace, None, None]:
    core_v1_api = kubernetes.client.CoreV1Api()

    list_namespace_result = core_v1_api.list_namespace(label_selector=label_selector)
    for namespace in list_namespace_result.items:
        yield namespace


def list_version_resources(*,
                           namespace_label_selector: str = '',
                           label_selector: str = '') -> Generator[Any, None, None]:
    custom_objects_api = kubernetes.client.CustomObjectsApi()

    for namespace in list_namespaces(label_selector=namespace_label_selector):
        list_version_result = custom_objects_api.list_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                                               version=BENJI_VERSIONS_API_VERSION,
                                                                               plural=BENJI_VERSIONS_API_PLURAL,
                                                                               namespace=namespace.metadata.name,
                                                                               label_selector=label_selector)

        for version_resource in list_version_result['items']:
            yield version_resource


def get_version_resource(name: str, namespace: str) -> Any:
    custom_objects_api = kubernetes.client.CustomObjectsApi()
    return custom_objects_api.get_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                           version=BENJI_VERSIONS_API_VERSION,
                                                           plural=BENJI_VERSIONS_API_PLURAL,
                                                           namespace=namespace,
                                                           name=name)


def build_version_labels_rbd(*,
                             pvc,
                             pv,
                             pool: str,
                             image: str,
                             cluster_name: str = 'ceph',
                             cluster_fsid: str) -> Dict[str, str]:
    version_labels = {
        LABEL_INSTANCE: settings.benji_instance,
        LABEL_K8S_PVC_NAMESPACE: pvc.metadata.namespace,
        LABEL_K8S_PVC_NAME: pvc.metadata.name,
        LABEL_K8S_PV_NAME: pv.metadata.name,
        LABEL_K8S_STORAGE_CLASS_NAME: pv.spec.storage_class_name,
        # RBD specific
        LABEL_K8S_PV_TYPE: PV_TYPE_RBD,
        LABEL_RBD_CLUSTER_FSID: cluster_fsid,
        LABEL_RBD_IMAGE_SPEC: f'{pool}/{image}',
    }

    return version_labels


# This is taken from https://github.com/kubernetes-client/python/pull/855 with minimal changes.
#
# Copyright 2019 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def parse_quantity(quantity: Union[str, int, float]) -> float:
    """
    Parse kubernetes canonical form quantity like 200Mi to an float number.
    Supported SI suffixes:
    base1024: Ki | Mi | Gi | Ti | Pi | Ei
    base1000: m | "" | k | M | G | T | P | E

    Input:
    quanity: string. kubernetes canonical form quantity

    Returns:
    float

    Raises:
    ValueError on invalid or unknown input
    """
    exponents = {"m": -1, "K": 1, "k": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}
    pattern = r"^(\d+)([^\d]{1,2})?$"

    if isinstance(quantity, (int, float)):
        return float(quantity)

    quantity = str(quantity)

    res = re.match(pattern, quantity)
    if not res:
        raise ValueError("{} did not match pattern {}".format(quantity, pattern))
    number, suffix = res.groups()
    number_float = float(number)

    if suffix is None:
        return number_float

    suffix = res.groups()[1]

    if suffix.endswith("i"):
        base = 1024
    elif len(suffix) == 1:
        base = 1000
    else:
        raise ValueError("{} has unknown suffix".format(quantity))

    # handle SI inconsistency
    if suffix == "ki":
        raise ValueError("{} has unknown suffix".format(quantity))

    if suffix[0] not in exponents:
        raise ValueError("{} has unknown suffix".format(quantity))

    exponent = exponents[suffix[0]]
    return number_float * (base**exponent)


# End of included content
