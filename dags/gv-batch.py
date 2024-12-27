import json

# Airflow Core Module
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Airflow Provider
from airflow.providers.http.operators.http import HttpOperator

# Airflow Task API
from airflow.decorators import task

# Pydantic & typing
from pydantic import BaseModel, ConfigDict, Field
from typing import List, Optional, Union

from datetime import datetime

class customerInfoModel(BaseModel):
    """Pydantic data model for customer informations.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    gender: Optional[str] | None = None
    birthday: Optional[str] | None = None
    birth_year: Optional[int] | None = None
    birth_month: Optional[int] | None = None
    birth_day: Optional[int] | None = None
    custom_data: Optional[List[dict]] | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

class paymentFeeModel(BaseModel):
    """Pydantic data model for payment.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    cents: int
    currency_symbol: str
    currency_iso: str
    label: str
    dollars: int

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

class paymentSlipsModel(BaseModel):
    """Pydantic data model for payment slip.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    enabled: bool
    days_of_guest_view: int

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

class orderDeliveryModel(BaseModel):
    """Pydantic data model for order delivery.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    delivery_option_id: str
    platform: str
    status: str
    delivery_status: str
    name_translations: dict
    total: paymentFeeModel
    shipped_at: str | None
    arrived_at: str | None
    collected_at: str | None
    returned_at: str | None
    remark: str | None
    request_accepted_at: str | None
    request_authorized_at: str | None
    request_submitted_at: str | None
    requested_fmt_at: str | None
    require_expired_upload: bool
    require_storeclosed_upload: bool
    return_order_id: str | None
    store_closed_at: str | None
    storeclosed_upload_at: str | None
    delivery_type: Optional[str] | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class orderPaymentModel(BaseModel):
    """Pydantic data model for order payment.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    id: str
    payment_method_id: str
    payment_type: str
    name_translations: dict
    status: str
    payment_fee: paymentFeeModel
    total: paymentFeeModel
    paid_at: str | None
    updated_at: str
    created_at: str
    payment_data: dict
    last_four_digits: str
    ref_payment_id: str | None
    payment_slips_setting: Optional[paymentSlipsModel] | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

class deliveryAddressModel(BaseModel):
    """Pydantic data model for delivery address.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    country_code: str
    country: str
    city: Optional[str] | None = None
    state: Optional[str] | None = None
    postcode: Optional[str] | None = None
    address_1: Optional[str] | None = None
    address_2: Optional[str] | None = None
    district: Optional[str] | None = None
    key: Optional[str] | None = None
    layer1: Optional[str] | None = None
    layer2: Optional[str] | None = None
    layer3: Optional[str] | None = None
    logistic_codes: Optional[List[str]] | None = None
    recipient_name: str
    recipient_phone: str
    recipient_phone_country_code: str
    remarks: str | None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class deliveryDataModel(BaseModel):
    """Pydantic data model for delivery data.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    hk_sfplus_home_region: str | None
    location_code: str | None
    location_name: str | None
    location_short_name: str | None
    name_translations: dict | None
    store_address: str | None
    url: str | None
    tracking_number: str | None
    scheduled_delivery_date: str | None
    time_slot_key: str | None
    time_slot_translations: Union[dict, str] | None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)



class invoiceModel(BaseModel):
    """Pydantic data model for invoice.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    tax_id: str
    mailing_address: str
    invoice_type: str
    buyer_name: str
    carrier_type: str
    carrier_number: str
    n_p_o_b_a_n: str
    invoice_tax_type: str
    invoice_number: str
    invoice_status: str
    invoice_date: Optional[str] | None = None
    invoice_cancelled_at: Optional[str] = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

class orderInformationModel(BaseModel):
    """Pydantic data model for get orders api response.

    Args:
        BaseModel (object): Pydantic base model object.
    """

    id: str
    order_number: int
    status: str
    order_remarks: str
    order_payment: orderPaymentModel
    order_delivery: orderDeliveryModel
    delivery_address: deliveryAddressModel
    delivery_data: deliveryDataModel
    customer_id: str
    customer_name: str
    customer_email: Optional[str] | None = None
    customer_phone: str
    customer_info: Optional[customerInfoModel] | dict = None
    currency_iso: str
    subtotal: paymentFeeModel
    order_discount: paymentFeeModel
    user_credit: paymentFeeModel
    total_tax_fee: paymentFeeModel
    total: paymentFeeModel
    invoice: invoiceModel
    subtotal_items: List[dict]
    custom_data: List
    custom_discount_items: List
    affiliate_data: dict
    ref_order_id: str
    ref_customer_id: str
    channel: dict
    parent_order_id: str
    child_order_ids: List
    split_at: Optional[str] | None = None
    confirmed_at: Optional[str] | None = None
    created_by: str
    updated_at: str
    created_at: str
    skip_fulfillment: bool
    utm_data: dict
    ga_tracked: bool
    order_comments: List
    order_notes: List


class orderPaginationModel(BaseModel):
    current_page: int
    per_page: int
    total_pages: int
    total_count: int

class orderErrorsModel(BaseModel):
    notfound: List[str]
    gone: List[str]

class getOrdersResponseModel(BaseModel):
    error: Optional[orderErrorsModel] | None = None
    items: List[orderInformationModel]
    pagination: orderPaginationModel
    
@task(task_id='python_test')
def upload_gcs(ti, **context):
    data = ti.xcom_pill(task_ids='get-order-task-payload')
    
    try:
        data = getOrdersResponseModel(**json.loads(data))
    except Exception as e:
        raise e

    print(data)

    
with DAG(
    dag_id='gv-sl-get-order',
    start_date=datetime(2024,1,1),
    catchup=False,
    schedule="1/10 * * * *",
):
    token = Variable.get('shopline-api-token')
    get_payload = HttpOperator(
        task_id='get-order-task-payload',
        http_conn_id='shopline-api',
        endpoint="/v1/orders",
        method='GET',
        headers={
            "Content-Type": "application/json",
            'Authorization': f'Bearer {token}',
        },
        log_response=True,
    )



    get_payload >> upload_gcs()
