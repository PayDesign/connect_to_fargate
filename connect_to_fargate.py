#!/usr/bin/env python3
import argparse
import sys
import subprocess
import os
import traceback
import logging
import datetime
import signal
import shlex
import shutil
import json
import glob
import configparser

SSO_SESSION_INVALID_MARKERS = (
  'The SSO session associated with this profile has expired or is otherwise invalid.',
  'Token has expired and refresh failed',
)

# ログ出力設定関数
def setLogger():
  script_name = __file__.split('/')[-1]
  log_dir_name = os.path.join(get_app_dir(), 'log')
  os.makedirs(log_dir_name, exist_ok=True)
  log_dir_base = log_dir_name + '/'

  dt = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
  logfile_name = log_dir_base + '{}_{}.log'.format(script_name, dt)

  logger = logging.getLogger(script_name)
  logger.setLevel(logging.INFO)

  fmt = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
  handler = logging.FileHandler(logfile_name)
  handler.setLevel(logging.INFO)
  handler.setFormatter(fmt)
  logger.addHandler(handler)

  fmt_stdout = logging.Formatter('%(message)s')
  handler_stdout= logging.StreamHandler()
  handler_stdout.setLevel(logging.INFO)
  handler_stdout.setFormatter(fmt_stdout)
  logger.addHandler(handler_stdout)

  return logger, logfile_name


def read_log_tail(logfile, max_chars=8000):
  if not os.path.exists(logfile):
    return ''
  with open(logfile, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()
  return content[-max_chars:]


def build_execute_command_error_message(output):
  if (
    'AccessDeniedException' in output and
    'ecs:ExecuteCommand' in output and
    'Fargate_Access_SourceIp' in output
  ):
    return '許可された Source IP 以外からのアクセスです。AWS VPNに接続しているか確認してください。'
  return None


def get_app_name():
  return os.path.splitext(os.path.basename(__file__))[0]


def get_app_dir():
  return os.path.join(os.path.expanduser('~'), '.{}'.format(get_app_name()))


def get_aws_config_path():
  return os.path.join(os.path.expanduser('~'), '.aws', 'config')


def get_aws_sso_cache_dir():
  return os.path.join(os.path.expanduser('~'), '.aws', 'sso', 'cache')


def load_json_file(path, default):
  if not os.path.exists(path):
    return default
  with open(path, 'r', encoding='utf-8') as f:
    return json.load(f)


def normalize_url(url):
  if not url:
    return None
  return url.rstrip('/')


def parse_aws_timestamp(value, label):
  if not value:
    raise Exception('日時が空です: {}'.format(label))
  normalized_value = value
  if normalized_value.endswith('Z'):
    normalized_value = normalized_value[:-1] + '+00:00'
  try:
    parsed = datetime.datetime.fromisoformat(normalized_value)
  except ValueError:
    raise Exception('日時形式が不正です: {} ({})'.format(label, value))
  if parsed.tzinfo is None:
    parsed = parsed.replace(tzinfo=datetime.timezone.utc)
  return parsed.astimezone(datetime.timezone.utc)


def load_aws_profile_sso_settings(profile_name):
  aws_config_path = get_aws_config_path()
  if not os.path.exists(aws_config_path):
    raise Exception('AWS config が見つかりません: {}'.format(aws_config_path))

  config = configparser.RawConfigParser()
  config.read(aws_config_path, encoding='utf-8')

  profile_section = 'default' if profile_name == 'default' else 'profile {}'.format(profile_name)
  if not config.has_section(profile_section):
    raise Exception('AWS config にプロファイルが見つかりません: {}'.format(profile_section))

  start_url = config.get(profile_section, 'sso_start_url', fallback=None)
  issuer_url = config.get(profile_section, 'sso_issuer_url', fallback=None)
  sso_region = config.get(profile_section, 'sso_region', fallback=None)
  session_name = config.get(profile_section, 'sso_session', fallback=None)

  if session_name:
    session_section = 'sso-session {}'.format(session_name)
    if not config.has_section(session_section):
      raise Exception('AWS config に SSO セッション定義が見つかりません: {}'.format(session_section))
    start_url = start_url or config.get(session_section, 'sso_start_url', fallback=None)
    issuer_url = issuer_url or config.get(session_section, 'sso_issuer_url', fallback=None)
    sso_region = sso_region or config.get(session_section, 'sso_region', fallback=None)

  if not start_url and not issuer_url:
    raise Exception(
      'AWS config の SSO 設定が不足しています。`sso_start_url` または `sso_issuer_url` を確認してください: {}'.format(
        profile_section
      )
    )
  if not sso_region:
    raise Exception('AWS config の `sso_region` が未設定です: {}'.format(profile_section))

  return {
    'profile_section': profile_section,
    'sso_start_url': start_url,
    'sso_issuer_url': issuer_url,
    'sso_region': sso_region,
  }


def get_sso_cached_login(profile_name):
  settings = load_aws_profile_sso_settings(profile_name)
  cache_dir = get_aws_sso_cache_dir()
  if not os.path.isdir(cache_dir):
    return None

  profile_urls = {
    url for url in (
      normalize_url(settings['sso_start_url']),
      normalize_url(settings['sso_issuer_url']),
    ) if url
  }
  candidates = []
  for path in glob.glob(os.path.join(cache_dir, '*.json')):
    try:
      cache = load_json_file(path, None)
    except Exception:
      continue
    if not isinstance(cache, dict):
      continue
    if not cache.get('accessToken') or not cache.get('expiresAt'):
      continue
    cache_region = cache.get('region')
    if cache_region and cache_region != settings['sso_region']:
      continue
    cache_urls = {
      url for url in (
        normalize_url(cache.get('startUrl')),
        normalize_url(cache.get('issuerUrl')),
      ) if url
    }
    if not cache_urls or cache_urls.isdisjoint(profile_urls):
      continue
    candidates.append({
      'path': path,
      'expires_at': parse_aws_timestamp(cache['expiresAt'], path),
    })

  if not candidates:
    return None
  return max(candidates, key=lambda candidate: candidate['expires_at'])


def format_timedelta(delta):
  total_seconds = int(delta.total_seconds())
  sign = '-' if total_seconds < 0 else ''
  total_seconds = abs(total_seconds)
  hours, rem = divmod(total_seconds, 3600)
  minutes, seconds = divmod(rem, 60)
  if hours:
    return '{}{}h{}m'.format(sign, hours, minutes)
  if minutes:
    return '{}{}m{}s'.format(sign, minutes, seconds)
  return '{}{}s'.format(sign, seconds)


def is_invalid_sso_session_error(*texts):
  normalized_texts = [text for text in texts if text]
  return any(
    marker in text
    for marker in SSO_SESSION_INVALID_MARKERS
    for text in normalized_texts
  )


def selected_answer(choices, message):
  import json
  import string
  import inquirer
  from inquirer import themes
  from inquirer.render.console import ConsoleRender, List
  from readchar import key

  # CTRL_MAP["B"]はkey.CTRL_Bでも良いのだけれどAからZまで全部は定義されていなかったので
  CTRL_MAP = {c: chr(i) for i, c in enumerate(string.ascii_uppercase, 1)}

  # emacs風のキーバインド
  class ExtendedConsoleRender(ConsoleRender):
      def render_factory(self, question_type):
          if question_type == "list":
              return ExtendedList
          return super().render_factory(question_type)

  class ExtendedList(List):
      def process_input(self, pressed):
          # emacs style
          if pressed in (CTRL_MAP["B"], CTRL_MAP["P"]):
              pressed = key.UP
          elif pressed in (CTRL_MAP["F"], CTRL_MAP["N"]):
              pressed = key.DOWN
          elif pressed == CTRL_MAP["G"]:
              pressed = CTRL_MAP["C"]
          elif pressed == CTRL_MAP["A"]:
              self.current = 0
              return
          elif pressed == CTRL_MAP["G"]:
              self.current = len(self.question.choices) - 1
              return

          # vi style
          if pressed in ("k", "h"):
              pressed = key.UP
          elif pressed in ("j", "l"):
              pressed = key.DOWN
          elif pressed == "q":
              pressed = key.CTRL_C

          # effect (rendering)
          super().process_input(pressed)

  questions = [
      inquirer.List(
          "answer",
          message=message,
          choices=choices,
          carousel=True,
      )
  ]
  answer = inquirer.prompt(questions, render=ExtendedConsoleRender(theme=themes.GreenPassion()))
  return json.loads(json.dumps(answer))['answer']


def get_ecs_client():
  import boto3

  session = boto3.session.Session(profile_name = os.environ['AWS_PROFILE'])
  return session.client('ecs')


def build_parser():
  parser = argparse.ArgumentParser(
    prog='connect_to_fargate.py',
    description='AWS SSO の状態を確認し、必要に応じてログインしてから Fargate に接続します。',
    epilog=(
      'Examples:\n'
      '  connect_to_fargate.py -p profile\n'
      '  connect_to_fargate.py -p profile -c cluster -s service -t app -f\n'
      '  connect_to_fargate.py --profile profile --cluster cluster --task task-id --container app'
    ),
    formatter_class=argparse.RawTextHelpFormatter,
  )
  parser.add_argument('-p', '--profile', help='AWS プロファイル名。未指定時は AWS_PROFILE を利用')
  parser.add_argument('-c', '--cluster', help='クラスター名')
  parser.add_argument('-s', '--service', help='サービス名')
  parser.add_argument('--task', help='タスク名')
  parser.add_argument('-t', '--container', help='コンテナ名')
  parser.add_argument('--cmd', default='/bin/bash', help='コンテナで実行するコマンド')
  parser.add_argument('-f', '--force', action='store_true', help='接続確認なしでログインを行う')
  parser.add_argument(
    '--force-login',
    action='store_true',
    help='SSO セッションを強制的に再ログインする（aws sso logout -> aws sso login）',
  )
  return parser


def get_aws_cli_path():
  aws_cli = shutil.which('aws')
  if aws_cli:
    return aws_cli
  fallback = '/usr/local/bin/aws'
  if os.path.exists(fallback):
    return fallback
  raise Exception('aws cli が見つかりません。PATH または /usr/local/bin/aws を確認してください。')


def resolve_aws_profile(profile_name):
  resolved_profile = profile_name or os.environ.get('AWS_PROFILE')
  if not resolved_profile:
    raise Exception('AWS プロファイルが未指定です。`-p/--profile` または `AWS_PROFILE` を指定してください。')
  os.environ['AWS_PROFILE'] = resolved_profile
  return resolved_profile


def run_aws_sso_login(logger, profile_name):
  aws_cli = get_aws_cli_path()
  login_cmd = [aws_cli, 'sso', 'login', '--profile', profile_name]
  logger.info('`aws sso login --profile {}` を実行します'.format(profile_name))
  login_result = subprocess.run(login_cmd)
  if login_result.returncode != 0:
    raise Exception('aws sso login に失敗しました。profile={}'.format(profile_name))
  logger.info('AWS SSO ログインが完了しました: profile={}'.format(profile_name))


def run_aws_sso_logout(logger):
  aws_cli = get_aws_cli_path()
  logout_cmd = [aws_cli, 'sso', 'logout']
  logger.info('`aws sso logout` を実行します')
  logout_result = subprocess.run(logout_cmd)
  if logout_result.returncode != 0:
    logger.warning('aws sso logout は失敗しましたが、続けて aws sso login を実行します。')
    return
  logger.info('AWS SSO ログアウトが完了しました')


def ensure_aws_sso_login(logger, profile_name, force_login):
  if force_login:
    logger.info('`--force-login` が指定されたため、SSO セッションを再作成します')
    run_aws_sso_logout(logger)
    run_aws_sso_login(logger, profile_name)
    return

  cached_login = get_sso_cached_login(profile_name)
  if cached_login is None:
    logger.info(
      '一致する AWS SSO キャッシュが見つからないため、ログインを実行します: profile={}, cache_dir={}'.format(
        profile_name,
        get_aws_sso_cache_dir(),
      )
    )
    run_aws_sso_login(logger, profile_name)
    return

  expires_at = cached_login['expires_at']
  remaining = expires_at - datetime.datetime.now(datetime.timezone.utc)
  if remaining <= datetime.timedelta(seconds=0):
    logger.info(
      'AWS SSO キャッシュの有効期限を超過したため、再ログインします: profile={}, expires_at={}, cache={}'.format(
        profile_name,
        expires_at.isoformat(),
        cached_login['path'],
      )
    )
    run_aws_sso_logout(logger)
    run_aws_sso_login(logger, profile_name)
    return

  logger.info(
    'AWS SSO セッションは有効です: profile={}, expires_at={}, remaining={}, cache={}'.format(
      profile_name,
      expires_at.isoformat(),
      format_timedelta(remaining),
      cached_login['path'],
    )
  )


def recover_invalid_sso_session(logger, profile_name):
  logger.warning(
    'SSO セッション失効を検知したため、AWS SSO キャッシュを再取得します: profile={}, cache_dir={}'.format(
      profile_name,
      get_aws_sso_cache_dir(),
    )
  )
  run_aws_sso_logout(logger)
  run_aws_sso_login(logger, profile_name)


# クラスター名のチェック
def checkCluster(cluster_name):
  ecs = get_ecs_client()

  cluster_list = []
  for clusterArn in ecs.list_clusters()['clusterArns']:
    cluster = clusterArn.split('/')[len(clusterArn.split('/')) - 1]
    cluster_list.append(cluster)

  if cluster_name in cluster_list:
    return True
  else :
    return False

# クラスター名の設定
def setCluster(logger):
  ecs = get_ecs_client()

  cluster_list = []
  for clusterArn in ecs.list_clusters()['clusterArns']:
    cluster = clusterArn.split('/')[len(clusterArn.split('/')) - 1]
    cluster_list.append(cluster)

  cluster_name = selected_answer(cluster_list, "接続先が存在するクラスター名を選択してください")

  if checkCluster(cluster_name):
    logger.info('クラスター名: {}\n'.format(cluster_name))
    return cluster_name
  else :
    raise Exception('正しいクラスター名を選択してください。')

# サービス名のチェック
def checkService(cluster_name, service_name):
  ecs = get_ecs_client()

  ## スタンドアロンタスクを指定したい場合はチェックを行わない
  if service_name is None:
    return True

  service_list = []
  next_token = None
  while True:
    if next_token:
      response = ecs.list_services(
        cluster=cluster_name,
        maxResults=100,
        nextToken=next_token
      )
    else:
      response = ecs.list_services(
        cluster=cluster_name,
        maxResults=100
      )
    service_arns = response['serviceArns']
    if not service_arns:
      break
    # 10件ずつ describe_services に渡す
    for i in range(0, len(service_arns), 10):
      batch_arns = service_arns[i:i + 10]
      describe_response = ecs.describe_services(
        cluster=cluster_name,
        services=batch_arns
      )
      for service in describe_response['services']:
        launch_type = service.get('launchType')
        if launch_type not in ['EC2', 'EXTERNAL']:
          service_list.append(service['serviceName'])
    next_token = response.get('nextToken')
    if not next_token:
      break
  return service_name in service_list

# サービス名の設定
def setService(logger, cluster_name):
  ecs = get_ecs_client()
  service_list = []
  next_token = None
  while True:
    if next_token:
      response = ecs.list_services(
        cluster=cluster_name,
        maxResults=100,
        nextToken=next_token
      )
    else:
      response = ecs.list_services(
        cluster=cluster_name,
        maxResults=100
      )
    service_arns = response['serviceArns']
    if not service_arns:
      break
    # 10 個ずつ分割して describe_services を呼び出し
    for i in range(0, len(service_arns), 10):
      batch_arns = service_arns[i:i + 10]
      describe_response = ecs.describe_services(
        cluster=cluster_name,
        services=batch_arns
      )
      for service in describe_response['services']:
        launch_type = service.get('launchType')
        if launch_type not in ['EC2', 'EXTERNAL']:
          service_list.append(service['serviceName'])
    next_token = response.get('nextToken')
    if not next_token:
      break
  # スタンドアロンタスク用の選択肢を追加
  service_list.append('[standalone-tasks]')
  # サービス選択
  service_name = selected_answer(service_list, "接続先が存在するサービス名を選択してください")
  if service_name == '[standalone-tasks]':
    logger.info('サービス名: {}\n'.format(service_name))
    return None
  elif checkService(cluster_name, service_name):
    logger.info('サービス名: {}\n'.format(service_name))
    return service_name
  else:
    raise Exception('正しいサービス名を選択してください。')

# タスク名のチェック
def checkTask(cluster_name, service_name, task_name):
  ecs = get_ecs_client()

  task_list = []
  if service_name is None:
    for task_arn in ecs.list_tasks(
      cluster = cluster_name,
      desiredStatus = 'RUNNING',
      maxResults = 100
    )['taskArns']:
      task = task_arn.split('/')[len(task_arn.split('/')) - 1]
      task_list.append(task)
  else:
    for task_arn in ecs.list_tasks(
      cluster = cluster_name,
      serviceName = service_name,
      desiredStatus = 'RUNNING',
      maxResults = 100
    )['taskArns']:
      task = task_arn.split('/')[len(task_arn.split('/')) - 1]
      task_list.append(task)

  if task_name in task_list:
    return True
  else :
    return False

# タスク名の設定
def setTask(logger, cluster_name, service_name):
  ecs = get_ecs_client()

  task_list = []
  if service_name is None:
    task_arn = ecs.list_tasks(
      cluster = cluster_name,
      desiredStatus = 'RUNNING',
      maxResults = 100
    )['taskArns']
    task_details = ecs.describe_tasks(cluster=cluster_name, tasks=task_arn)
    # スタンドアロンタスクをフィルタリング
    for task in task_details['tasks']:
      if not task['group'].startswith('service:'):
        task_name = task['taskArn'].split('/')[len(task['taskArn'].split('/')) - 1]
        task_list.append(task_name)
  else:
    for task_arn in ecs.list_tasks(
      cluster = cluster_name,
      serviceName = service_name,
      desiredStatus = 'RUNNING',
      maxResults = 100
    )['taskArns']:
      task_name = task_arn.split('/')[len(task_arn.split('/')) - 1]
      task_list.append(task_name)
  if len(task_list) == 0:
    logger.error('タスクが存在しません')
    raise Exception('最初からやりなおしてください。')
  else:
    task_name = selected_answer(task_list, "接続先が存在するタスク名を選択してください")

  if checkTask(cluster_name, service_name, task_name):
    logger.info('タスク名: {}\n'.format(task_name))
    return task_name
  else :
    raise Exception('正しいタスク名を選択してください。')

# コンテナ名のチェック
def checkContainer(cluster_name, task_name, container_name):
  ecs = get_ecs_client()

  task_detail_list = ecs.describe_tasks(
    cluster = cluster_name,
    tasks=[
      task_name
    ],
  )
  container_name_list = []
  for task in task_detail_list['tasks']:
    for container in task['overrides']['containerOverrides']:
      container_name_list.append(container['name'])

  if container_name in container_name_list:
    return True
  else :
    return False

# コンテナ名の設定
def setContainer(logger, cluster_name, task_name):
  ecs = get_ecs_client()

  container_list = []
  task_detail_list = ecs.describe_tasks(
    cluster = cluster_name,
    tasks=[
      task_name
    ],
  )
  for task in task_detail_list['tasks']:
    for container in task['overrides']['containerOverrides']:
      container_list.append(container['name'])

  container_name = selected_answer(container_list, "接続先のコンテナ名を選択してください")

  if checkContainer(cluster_name, task_name, container_name):
    logger.info('コンテナ名: {}\n'.format(container_name))
    return container_name
  else :
    raise Exception('正しいコンテナ名を選択してください。')

# FARGATEへ接続
def ecsExecute(logger, cluster_name, service_name, task_name, container_name, shell_cmd, logfile, force_connect):
  ## 接続先確認のメッセージを出力
  str  = '以下のFargateに接続します\n'
  str += '----------------------------------------\n'
  str += 'クラスター名: {}\n'.format(cluster_name)
  str += 'サービス名: {}\n'.format(service_name)
  str += 'タスク名: {}\n'.format(task_name)
  str += 'コンテナ名: {}\n'.format(container_name)
  str += '----------------------------------------\n'
  logger.info(str)
  if force_connect == False:
    is_exec= selected_answer(['yes', 'no'], "こちらに接続してよろしいですか")

  if force_connect == True or is_exec.startswith('y'):
    #session = boto3.session.Session(profile_name = os.environ['AWS_PROFILE'])
    #ecs = session.client('ecs')
    #ecs.execute_command(
    #  cluster = cluster_name,
    #  container = container_name,
    #  command = '/bin/bash',
    #  interactive = True,
    #  task = task_name
    #)
    #/bin/bashの場合セッションが切れてしまうためsubprocessを利用する方式に変更
    logger.info('Fargateにログインします')
    aws_cli = get_aws_cli_path()
    cmd  = 'set -o pipefail; {} ecs execute-command '.format(shlex.quote(aws_cli))
    cmd += '--cluster {} '.format(shlex.quote(cluster_name))
    cmd += '--task {} '.format(shlex.quote(task_name))
    cmd += '--container {} '.format(shlex.quote(container_name))
    cmd += '--interactive --command {} 2>&1 | tee {}'.format(
      shlex.quote(shell_cmd),
      shlex.quote(logfile),
    )

    ## Ctrl+C(SIGINTシグナル)を無視
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    ## subprocess実行
    out = subprocess.run(
      ['/bin/bash', '-lc', cmd],
      text=True,
      stdin=sys.stdin,
      stdout=sys.stdout,
      stderr=sys.stderr,
    )
    if out.returncode != 0:
      error_output = read_log_tail(logfile)
      friendly_message = build_execute_command_error_message(error_output)
      if friendly_message:
        logger.error(friendly_message)
      raise Exception('Fargateへの接続に失敗しました。')
    logger.info(out)
    logger.info('Fargateからログアウトしました')
  return

def view_help():
  print(build_parser().format_help().strip())


def run_main_flow(args, logger, logfile):
  profile_name = resolve_aws_profile(args.profile)
  ensure_aws_sso_login(logger, profile_name, args.force_login)

  ## 初期値の定義
  cluster_name = args.cluster or ''
  service_name = args.service if args.service is not None else ''
  task_name = args.task or ''
  container_name = args.container or ''
  shell_cmd = args.cmd
  force_connect = args.force
  logger.info('処理を開始します')
  logger.info('AWS プロファイル: {}\n'.format(profile_name))

  ## 引数で指定がない場合に設定する関数を実行する
  if cluster_name == '':
    cluster_name = setCluster(logger)

  if service_name == '':
    if checkCluster(cluster_name):
      service_name = setService(logger, cluster_name)
    else:
      raise Exception('正しいクラスター名を指定してください。')

  if task_name == '':
    if checkCluster(cluster_name) and \
       checkService(cluster_name, service_name):
      task_name = setTask(logger, cluster_name, service_name)
    else:
      raise Exception('正しいクラスター名またはサービス名を指定してください。')

  if container_name == '':
    if checkCluster(cluster_name) and \
       checkService(cluster_name, service_name) and \
       checkTask(cluster_name, service_name, task_name):
      container_name = setContainer(logger, cluster_name, task_name)
    else:
      raise Exception('正しいクラスター名またはサービス名またはタスク名を指定してください。')

  ## cluster_name, service_name, task_name, container_nameの実在確認（最終）
  if not checkCluster(cluster_name):
    raise Exception('正しいクラスター名を指定してください。')
  if not checkService(cluster_name, service_name):
    raise Exception('正しいサービス名を指定してください。')
  if not checkTask(cluster_name, service_name, task_name):
    raise Exception('正しいタスク名を指定してください。')
  if not checkContainer(cluster_name, task_name, container_name):
    raise Exception('正しいコンテナ名を指定してください。')

  ## Fargate接続関数を実行する
  ecsExecute(logger, cluster_name, service_name, task_name, container_name, shell_cmd, logfile, force_connect)


# 主処理
def main(argv=None):
  logger = None
  logfile = None
  try:
    parser = build_parser()
    args = parser.parse_args(argv)
    logger, logfile = setLogger()
    try:
      run_main_flow(args, logger, logfile)
    except Exception as e:
      diagnostic_text = '{}\n{}\n{}'.format(
        e,
        traceback.format_exc(),
        read_log_tail(logfile),
      )
      profile_name = os.environ.get('AWS_PROFILE') or args.profile
      if (
        profile_name and
        is_invalid_sso_session_error(diagnostic_text)
      ):
        logger.warning('AWS SSO セッション失効を検知したため、再ログイン後に1回だけ再試行します')
        recover_invalid_sso_session(logger, profile_name)
        run_main_flow(args, logger, logfile)
      else:
        raise
  except Exception as e:
    error_message = "処理を終了します\nエラー詳細: {}\n{}".format(e, traceback.format_exc())
    if logger:
      logger.error(error_message)
    else:
      print(error_message, file=sys.stderr)
    exit(255)
  return

# 実行処理
if __name__ == "__main__":
  main(sys.argv[1:])
