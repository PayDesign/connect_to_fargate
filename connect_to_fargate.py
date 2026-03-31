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

DEFAULT_SSO_SESSION_DURATION_HOURS = 12

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


def get_app_name():
  return os.path.splitext(os.path.basename(__file__))[0]


def get_app_dir():
  return os.path.join(os.path.expanduser('~'), '.{}'.format(get_app_name()))


def get_config_path():
  return os.path.join(get_app_dir(), 'config.json')


def get_state_path():
  return os.path.join(get_app_dir(), 'state.json')


def load_json_file(path, default):
  if not os.path.exists(path):
    return default
  with open(path, 'r', encoding='utf-8') as f:
    return json.load(f)


def save_json_file(path, data):
  os.makedirs(os.path.dirname(path), exist_ok=True)
  with open(path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)


def load_sso_session_duration_hours():
  config = load_json_file(get_config_path(), {})
  duration_hours = config.get('sso_session_duration_hours', DEFAULT_SSO_SESSION_DURATION_HOURS)
  try:
    duration_hours = float(duration_hours)
  except (TypeError, ValueError):
    raise Exception(
      '設定ファイル `{}` の `sso_session_duration_hours` は時間単位の数値で指定してください。'.format(
        get_config_path()
      )
    )
  if duration_hours <= 0:
    raise Exception(
      '設定ファイル `{}` の `sso_session_duration_hours` は 0 より大きい値を指定してください。'.format(
        get_config_path()
      )
    )
  return duration_hours


def load_sso_state():
  return load_json_file(get_state_path(), {'profiles': {}})


def get_last_sso_login_at(profile_name):
  state = load_sso_state()
  profile_state = state.get('profiles', {}).get(profile_name, {})
  last_login_at = profile_state.get('last_sso_login_at')
  if not last_login_at:
    return None
  try:
    return datetime.datetime.fromisoformat(last_login_at)
  except ValueError:
    raise Exception(
      '状態ファイル `{}` の `last_sso_login_at` が不正です。'.format(get_state_path())
    )


def record_sso_login(profile_name, logged_in_at=None):
  state = load_sso_state()
  profiles = state.setdefault('profiles', {})
  profiles[profile_name] = {
    'last_sso_login_at': (logged_in_at or datetime.datetime.now(datetime.timezone.utc)).isoformat()
  }
  save_json_file(get_state_path(), state)

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
  record_sso_login(profile_name)
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


def ensure_aws_sso_login(logger, profile_name, session_duration_hours, force_login):
  last_login_at = get_last_sso_login_at(profile_name)
  logger.info(
    'SSO セッション維持時間: {}時間 (config: {})'.format(
      session_duration_hours,
      get_config_path(),
    )
  )

  if force_login:
    logger.info('`--force-login` が指定されたため、SSO セッションを再作成します')
    run_aws_sso_logout(logger)
    run_aws_sso_login(logger, profile_name)
    return

  if last_login_at is None:
    logger.info('前回の AWS SSO ログイン記録がないため、ログインを実行します')
    run_aws_sso_login(logger, profile_name)
    return

  if last_login_at.tzinfo is None:
    last_login_at = last_login_at.replace(tzinfo=datetime.timezone.utc)

  elapsed = datetime.datetime.now(datetime.timezone.utc) - last_login_at.astimezone(datetime.timezone.utc)
  session_limit = datetime.timedelta(hours=session_duration_hours)
  if elapsed >= session_limit:
    logger.info(
      '前回の AWS SSO ログインから {} を超過したため、再ログインします'.format(
        session_limit
      )
    )
    run_aws_sso_logout(logger)
    run_aws_sso_login(logger, profile_name)
    return

  logger.info(
    'AWS SSO ログイン記録は有効期間内です: profile={}, last_login_at={}'.format(
      profile_name,
      last_login_at.isoformat(),
    )
  )


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
    cmd  = '{} ecs execute-command '.format(shlex.quote(aws_cli))
    cmd += '--cluster {} '.format(shlex.quote(cluster_name))
    cmd += '--task {} '.format(shlex.quote(task_name))
    cmd += '--container {} '.format(shlex.quote(container_name))
    cmd += '--interactive --command {} | tee {}'.format(
      shlex.quote(shell_cmd),
      shlex.quote(logfile),
    )

    ## Ctrl+C(SIGINTシグナル)を無視
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    ## subprocess実行
    out = subprocess.run(cmd, text=True, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, shell=True)
    logger.info(out)
    logger.info('Fargateからログアウトしました')
  return

def view_help():
  print(build_parser().format_help().strip())

# 主処理
def main(argv=None):
  try:
    parser = build_parser()
    args = parser.parse_args(argv)
    logger, logfile = setLogger()
    profile_name = resolve_aws_profile(args.profile)
    session_duration_hours = load_sso_session_duration_hours()
    ensure_aws_sso_login(logger, profile_name, session_duration_hours, args.force_login)

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
      cluster_name    = setCluster(logger)

    if service_name == '':
      if checkCluster(cluster_name):
        service_name    = setService(logger, cluster_name)
      else :
        raise Exception('正しいクラスター名を指定してください。')

    if task_name == '':
      if checkCluster(cluster_name) and \
         checkService(cluster_name, service_name):
        task_name       = setTask(logger, cluster_name, service_name)
      else :
        raise Exception('正しいクラスター名またはサービス名を指定してください。')

    if container_name == '':
      if checkCluster(cluster_name) and \
         checkService(cluster_name, service_name) and \
         checkTask(cluster_name, service_name, task_name):
        container_name  = setContainer(logger, cluster_name, task_name)
      else :
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
  except Exception as e:
    logger.error("処理を終了します\nエラー詳細: {}\n{}".format(e, traceback.format_exc()))
    exit(255)
  return

# 実行処理
if __name__ == "__main__":
  main(sys.argv[1:])
