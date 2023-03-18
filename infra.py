from dataclasses import dataclass


@dataclass
class Settings:
    ...


class User:
    ...


class Role:
    ...


class Redshift:
    ...

class 


class Infrastructure:
    def __init__(self, settings: Settings):
        self.settings = settings


def main():
    infra = Infra(aws_key=AWS_KEY, aws_secret=AWS_SECRET)
    infra.iam.get_role(RoleName=ROLE_NAME)


if __name__ == "__main__":
    main()
